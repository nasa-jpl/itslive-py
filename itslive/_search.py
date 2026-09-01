"""Search backends for ITS_LIVE velocity data.

The unified entry point is :func:`search`::

    itslive.search(collection="itslive-granules", type="serverless", engine="duckdb", ...)
    itslive.search(type="pgstac", ...)

``type="serverless"`` queries partitioned STAC geoparquet directly from S3
(duckdb or rustac engine); ``type="pgstac"`` issues a STAC API search via
``pystac_client``.
"""

import collections
import datetime
import json
import logging
import math
import os

import numpy as np
import pyproj
import s3fs
from shapely.geometry import Polygon, box, mapping, shape

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_STAC_API_HREF = "https://stac.itslive.cloud"
WAREHOUSE_HREF = "s3://its-live-data/test-space/stac/catalog/warehouse"

_MISSIONS = ("landsatOLI", "sentinel1", "sentinel2")
_MISSION_TO_PLATFORM = {"sentinel1": "S1A", "sentinel2": "S2A", "landsatoli": "L8"}

# Default search window when start/end are not provided.
DEFAULT_START_DATE = "2000-01-01"
DEFAULT_END_DATE = "2025-12-31"


def bucket_cube_name_from_url(source_url: str) -> tuple[str, str]:
    """Extract bucket name and file URL from the given datacube URL.

    Args:
        source_url (str): AWS S3 URL of the datacube in Zarr format.

    Returns:
        tuple[str, str]: Bucket name and file URL.
    """
    source_url = source_url.replace("s3://", "")
    bucket_name, file_url = source_url.split("/", 1)
    logging.info(f"{bucket_name=} {file_url=}")
    return bucket_name, file_url


# ---------------------------------------------------------------------------
# Generic filter primitives
# ---------------------------------------------------------------------------

# Represents a single property constraint:
#   op    – CQL2 comparison operator string: "=", ">=", "<=", ">", "<", "!="
#   value – the literal value to compare against
PropertyFilter = collections.namedtuple("PropertyFilter", ["op", "value"])

# Convenient operator helpers so callers can be expressive without
# remembering raw operator strings.
EQ = lambda v: PropertyFilter("=", v)  # noqa: E731
GTE = lambda v: PropertyFilter(">=", v)  # noqa: E731
LTE = lambda v: PropertyFilter("<=", v)  # noqa: E731
GT = lambda v: PropertyFilter(">", v)  # noqa: E731
LT = lambda v: PropertyFilter("<", v)  # noqa: E731
NEQ = lambda v: PropertyFilter("!=", v)  # noqa: E731


def build_cql2_filters_from_dict(filters: dict) -> list:
    """
    Convert a ``{property_name: PropertyFilter}`` mapping to the CQL2
    expression list expected by ``filters_to_where`` and
    ``build_cql2_filter``.

    Args:
        filters (dict): Mapping of STAC property name to ``PropertyFilter``.

            Any property present in a STAC item's ``properties`` block is
            valid — ``"proj:code"``, ``"platform"``, ``"version"``,
            ``"updated"``, ``"created"``, ``"percent_valid_pixels"``, etc.

            Example::

                {
                    "percent_valid_pixels": GTE(85.0),
                    "proj:code":            EQ("EPSG:3413"),
                    "platform":             EQ("S2B"),
                    "version":              EQ("002"),
                    "updated":              GTE("2025-01-01T00:00:00Z"),
                }

    Returns:
        list: CQL2-style expression list.

    Raises:
        TypeError: If any value in *filters* is not a ``PropertyFilter``.
    """
    cql2_filters = []
    for prop, pf in filters.items():
        if not isinstance(pf, PropertyFilter):
            raise TypeError(
                f"Filter for '{prop}' must be a PropertyFilter, "
                f"got {type(pf).__name__}. "
                "Use PropertyFilter(op, value) or the EQ/GTE/… helpers."
            )
        cql2_filters.append(
            {
                "op": pf.op,
                "args": [{"property": prop}, pf.value],
            }
        )
    return cql2_filters


def build_default_filters(
    epsg_code: str,
    percent_valid_pixels: float = 1.0,
) -> dict:
    """
    Build the classic ``proj:code`` / ``percent_valid_pixels`` filter pair.

    Args:
        epsg_code (str): Numeric EPSG code, e.g. ``"3413"``.
        percent_valid_pixels (float): Minimum valid-pixel fraction.

    Returns:
        dict: ``{property_name: PropertyFilter}`` ready to pass as
            ``search(filters=...)``.

    Example::

        search(
            start="2022-01-01",
            end="2022-12-31",
            bbox=[-50, 65, -40, 75],
            filters=build_default_filters("3413", percent_valid_pixels=85.0),
        )
    """
    return {
        "percent_valid_pixels": GTE(percent_valid_pixels),
        "proj:code": EQ(f"EPSG:{epsg_code}"),
    }


def expr_to_sql(expr):
    """
    Transform a CQL2 expression into SQL.
    """
    op = expr["op"]
    left, right = expr["args"]

    def val_to_sql(val):
        if isinstance(val, dict) and "property" in val:
            prop = val["property"]
            if not prop.isidentifier():
                return f'"{prop}"'
            return prop
        elif isinstance(val, str):
            escaped = val.replace("'", "''")
            return f"'{escaped}'"
        else:
            return str(val)

    left_sql = val_to_sql(left)
    right_sql = val_to_sql(right)

    op_map = {
        "=": "=",
        "==": "=",
        ">=": ">=",
        "<=": "<=",
        ">": ">",
        "<": "<",
        "!=": "<>",
        "<>": "<>",
    }
    sql_op = op_map.get(op, op)
    return f"{left_sql} {sql_op} {right_sql}"


def filters_to_where(filters):
    """
    Convert a list of CQL2 expressions to a SQL WHERE clause string.
    """
    sql_parts = [expr_to_sql(f) for f in filters]
    return " AND ".join(sql_parts)


def build_cql2_filter(filters_list):
    """
    Wrap a CQL2 expression list into a single CQL2-JSON filter object.
    """
    if not filters_list:
        return None
    return (
        filters_list[0]
        if len(filters_list) == 1
        else {"op": "and", "args": filters_list}
    )


def path_exists(path: str) -> bool:
    """
    Check whether a local or S3 path exists.
    """
    if path.startswith("s3://"):
        fs = s3fs.S3FileSystem(anon=True)
        return fs.exists(path)
    else:
        return os.path.exists(path)


# ---------------------------------------------------------------------------
# Shared parameter translation
# ---------------------------------------------------------------------------


def build_roi(
    bbox: list[float] | None = None,
    polygon: list | None = None,
    geojson: dict | None = None,
) -> dict:
    """Build a GeoJSON geometry from friendly inputs.

    Priority: ``geojson`` > ``polygon`` > ``bbox``.

    Args:
        bbox: ``[min_lon, min_lat, max_lon, max_lat]``.
        polygon: Flat ``[lon, lat, ...]`` list or list of ``(lon, lat)`` pairs.
        geojson: GeoJSON geometry or ``Feature`` dict.

    Returns:
        dict: GeoJSON geometry.

    Raises:
        ValueError: If no geometry input is provided or the GeoJSON is invalid.
    """
    if geojson is not None:
        roi = geojson["geometry"] if geojson.get("type") == "Feature" else geojson
        try:
            shape(roi)  # raises if invalid
        except Exception as e:
            raise ValueError(f"Invalid GeoJSON geometry: {e}") from e
        return roi
    if polygon is not None:
        if polygon and not isinstance(polygon[0], (list, tuple)):
            it = iter(polygon)
            polygon = list(zip(it, it))
        return mapping(Polygon(polygon))
    if bbox is not None:
        return mapping(box(bbox[0], bbox[1], bbox[2], bbox[3]))
    raise ValueError("Search needs a bbox, polygon, geojson, or intersects geometry")


def _norm_date(value, default: str) -> str:
    """Normalize a start/end input (``datetime.date``, ISO string or None)."""
    if value is None:
        return default
    if isinstance(value, datetime.date):
        return value.isoformat()
    value = str(value)
    try:
        datetime.date.fromisoformat(value[:10])
    except ValueError:
        raise ValueError(
            f"Invalid date: {value!r}; expected YYYY-MM-DD or datetime.date"
        ) from None
    return value


def build_search_filters(
    percent_valid_pixels: int = 1,
    mission: str | None = None,
    min_interval: int | None = None,
    max_interval: int | None = None,
    filters: dict | None = None,
) -> tuple[dict, list]:
    """Translate friendly search parameters into property filters.

    Returns:
        tuple: ``(filters_dict, extra_cql2_exprs)``. The dict maps property
            names to ``PropertyFilter``; the list holds compound CQL2
            expressions (used when both ``min_interval`` and ``max_interval``
            are set, since a dict cannot hold duplicate keys).
    """
    param_filters = {}
    extra_cql2_exprs: list[dict] = []
    if percent_valid_pixels and percent_valid_pixels > 0:
        param_filters["percent_valid_pixels"] = GTE(percent_valid_pixels)
    if mission:
        platform = _MISSION_TO_PLATFORM.get(mission.lower())
        if platform:
            param_filters["platform"] = EQ(platform)
    # STAC property for time separation is "date_dt" (not min/max_interval_days).
    if min_interval is not None and max_interval is not None:
        extra_cql2_exprs.append(
            {
                "op": "and",
                "args": [
                    {"op": ">=", "args": [{"property": "date_dt"}, min_interval]},
                    {"op": "<=", "args": [{"property": "date_dt"}, max_interval]},
                ],
            }
        )
    elif min_interval is not None:
        param_filters["date_dt"] = GTE(min_interval)
    elif max_interval is not None:
        param_filters["date_dt"] = LTE(max_interval)
    if filters:
        param_filters.update({k: v for k, v in filters.items() if v is not None})
    return param_filters, extra_cql2_exprs


# ---------------------------------------------------------------------------
# Geoparquet (serverless) plumbing
# ---------------------------------------------------------------------------


def get_overlapping_grid_names(
    geojson_geometry: dict = {},
    base_href: str = "s3://its-live-data/test-space/stac/geoparquet/latlon",
    partition_type: str = "latlon",
    resolution: int = 2,
    overlap: str = "overlap",
    use_hive_partitions: bool = False,
):
    """
    Generates a list of S3 path prefixes corresponding to spatial grid tiles
    that overlap with the provided GeoJSON geometry. These paths are intended
    for discovering Parquet files in a spatially partitioned STAC dataset.

    Parameters
    ----------
    geojson_geometry : dict
        GeoJSON geometry dictionary for the region of interest.
    base_href : str
        Base S3 path where partitioned STAC data is stored.
    partition_type : str
        Partitioning scheme: ``"latlon"`` or ``"h3"``.
    resolution : int
        H3 resolution (only used when partition_type == ``"h3"``).
    overlap : str
        Overlap mode passed to ``h3shape_to_cells_experimental``
        (only used when partition_type == ``"h3"``).
    use_hive_partitions : bool
        When True and partition_type == ``"h3"``, build paths using
        Hive-style partition keys::

            {base_href}/grid=h3/level={resolution}/tile={hex_id}

        When False (default), the legacy integer-prefix scheme is used::

            {base_href}/{int(hex_id, 16)}

    Returns
    -------
    List[str]
        S3-style path prefixes pointing at the overlapping spatial
        partitions (no glob suffix; callers append ``**/*.parquet`` or
        ``year=YYYY/**/*.parquet`` as appropriate).
    """
    if partition_type == "latlon":

        def lat_prefix(lat):
            return f"N{abs(lat):02d}" if lat >= 0 else f"S{abs(lat):02d}"

        def lon_prefix(lon):
            return f"E{abs(lon):03d}" if lon >= 0 else f"W{abs(lon):03d}"

        geom = shape(geojson_geometry)

        if not geom.is_valid:
            geom = geom.buffer(0)

        minx, miny, maxx, maxy = geom.bounds

        lon_center_start = int(math.floor((minx - 5) / 10.0)) * 10
        lon_center_end = int(math.ceil((maxx + 5) / 10.0)) * 10
        lat_center_start = int(math.floor((miny - 5) / 10.0)) * 10
        lat_center_end = int(math.ceil((maxy + 5) / 10.0)) * 10

        grids = set()
        for lon_c in range(lon_center_start, lon_center_end + 1, 10):
            for lat_c in range(lat_center_start, lat_center_end + 1, 10):
                tile = box(lon_c - 5, lat_c - 5, lon_c + 5, lat_c + 5)
                if geom.intersects(tile):
                    name = f"{lat_prefix(lat_c)}{lon_prefix(lon_c)}"
                    grids.add(name)

        prefixes = [f"{base_href}/{p}/{i}" for p in _MISSIONS for i in list(grids)]
        return [path for path in prefixes if path_exists(path)]

    elif partition_type == "h3":
        import h3

        grids_hex = h3.h3shape_to_cells_experimental(
            h3.geo_to_h3shape(geojson_geometry),
            resolution,
            overlap,  # type: ignore[arg-type]
        )

        if use_hive_partitions:
            prefixes = [
                f"{base_href}/grid=h3/level={resolution}/tile={hex_id}"
                for hex_id in grids_hex
            ]
        else:
            # Legacy layout: hex cell ID converted to integer directory name.
            prefixes = [f"{base_href}/{int(hex_id, 16)}" for hex_id in grids_hex]

        return [prefix for prefix in prefixes if path_exists(prefix)]

    else:
        raise NotImplementedError(f"Partition {partition_type} not implemented.")


def _resolve_serverless_opts(base_catalog_href: str | None, **kwargs) -> tuple:
    """Resolve the geoparquet store and partition options from kwargs.

    Returns:
        tuple: ``(store, opts)`` where ``opts`` holds ``partition_type``,
            ``resolution``, ``overlap``, ``reduce_spatial_search``,
            ``use_hive_partitions`` and ``use_year_partitions``.

    Raises:
        ValueError: On invalid partition/resolution combinations.
        TypeError: On unknown keyword arguments.
    """
    store = base_catalog_href or WAREHOUSE_HREF
    is_warehouse = store.rstrip("/").endswith("warehouse")

    partition_type = kwargs.pop("partition_type", "h3")
    resolution = kwargs.pop("resolution", 1)
    use_hive_partitions = kwargs.pop("use_hive_partitions", True)
    if is_warehouse and partition_type == "h3":
        # The warehouse layout is Hive-partitioned only; integer-prefix
        # directories do not exist there.
        use_hive_partitions = True
    use_year_partitions = kwargs.pop("use_year_partitions", None)
    if use_year_partitions is None:
        # Only the warehouse layout carries year= partitions.
        use_year_partitions = (
            partition_type == "h3" and use_hive_partitions and is_warehouse
        )

    if partition_type == "h3":
        if is_warehouse and resolution != 1:
            raise ValueError(
                f"Invalid H3 resolution: {resolution}. The warehouse catalog "
                "only provides resolution 1; pass base_catalog_href explicitly "
                "for legacy catalogs (e.g. h3r2)."
            )
        if resolution not in (1, 2):
            raise ValueError(
                f"Invalid H3 resolution: {resolution}. "
                "Only resolutions 1 (coarser) and 2 (finer) are available."
            )
    elif partition_type != "latlon":
        raise ValueError(
            f"Invalid partition_type: {partition_type}. Must be 'h3' or 'latlon'."
        )

    opts = {
        "partition_type": partition_type,
        "resolution": resolution,
        "overlap": kwargs.pop("overlap", "bbox_overlap"),
        "reduce_spatial_search": kwargs.pop("reduce_spatial_search", True),
        "use_hive_partitions": use_hive_partitions,
        "use_year_partitions": bool(use_year_partitions),
    }
    if kwargs:
        raise TypeError(f"Unexpected search arguments: {sorted(kwargs)}")
    return store, opts


def _build_search_prefixes(
    store: str,
    roi: dict,
    opts: dict,
    start_date: str,
    end_date: str,
) -> list[str]:
    """Build the parquet glob list for a geoparquet search.

    Combines spatial partition pruning with (optional) ``year=`` Hive
    partition push-down derived from the date range.
    """
    if opts["partition_type"] == "latlon":
        if opts["reduce_spatial_search"]:
            prefixes = get_overlapping_grid_names(
                geojson_geometry=roi,
                base_href=store,
                partition_type="latlon",
            )
        else:
            prefixes = [f"{store}/{m}" for m in _MISSIONS]
    elif opts["use_hive_partitions"]:
        if opts["reduce_spatial_search"]:
            prefixes = get_overlapping_grid_names(
                geojson_geometry=roi,
                base_href=store,
                partition_type="h3",
                resolution=opts["resolution"],
                overlap=opts["overlap"],
                use_hive_partitions=True,
            )
        else:
            prefixes = [f"{store}/grid=h3/level={opts['resolution']}/tile=*"]
    else:
        prefixes = [store]

    if opts["use_year_partitions"]:
        years = range(int(start_date[:4]), int(end_date[:4]) + 1)
        return [f"{p}/year={y}/**/*.parquet" for p in prefixes for y in years]
    return [f"{p}/**/*.parquet" for p in prefixes]


def _sql_quote(value: str) -> str:
    """Escape a string literal for interpolation into a SQL statement."""
    return value.replace("'", "''")


def _ts_bound(value: str, end: bool = False) -> str:
    """Expand a bare ``YYYY-MM-DD`` string to a full UTC timestamp.

    Start bounds get ``T00:00:00Z``; end bounds get ``T23:59:59Z`` so the
    end date is inclusive. Full timestamps pass through unchanged.
    """
    value = value.strip()
    if len(value) == 10:
        value = f"{value}T23:59:59Z" if end else f"{value}T00:00:00Z"
    return value


# ---------------------------------------------------------------------------
# Backend iterators
# ---------------------------------------------------------------------------


def _iter_pgstac(
    store: str,
    collection: str,
    roi: dict,
    datetime_range: str,
    cql2_filter: dict | None,
    asset_type: str,
    limit: int | None = 10000,
):
    """Yield data asset hrefs from a pgstac-backed STAC API.

    A large ``limit`` is critical: without it pgstac pages at its default
    of 10 items/request, which means thousands of round-trips for large
    result sets.
    """
    import pystac_client

    client = pystac_client.Client.open(store)

    stac_search_kwargs = {
        "intersects": roi,
        "datetime": datetime_range,
        "collections": [collection],
    }
    if limit is not None:
        stac_search_kwargs["limit"] = limit
    if cql2_filter is not None:
        stac_search_kwargs["filter"] = cql2_filter
        stac_search_kwargs["filter_lang"] = "cql2-json"

    logging.info(f"Querying STAC API at {store}, collection={collection}")
    logging.info(f"STAC search kwargs: {stac_search_kwargs}")

    for item in client.search(**stac_search_kwargs).items():
        for asset in item.assets.values():
            if "data" in (asset.roles or []) and asset.href.endswith(asset_type):
                yield asset.href


def _iter_duckdb(
    prefixes: list[str],
    roi: dict,
    start_date: str,
    end_date: str,
    filters_sql: str,
    asset_type: str,
):
    """Yield data asset hrefs by querying geoparquet prefixes with duckdb.

    Results are consumed as an Arrow record-batch stream (constant memory,
    no pandas materialization); hrefs are yielded as each batch arrives.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    # Required for S3 reads; do not rely on auto-install (slow/unreliable).
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    # Row order is irrelevant (results are deduplicated/sorted downstream).
    con.execute("SET preserve_insertion_order=false")

    geojson_sql = _sql_quote(json.dumps(roi))
    temporal_sql = (
        f"start_datetime <= TIMESTAMPTZ '{_ts_bound(end_date, end=True)}' "
        f"AND end_datetime >= TIMESTAMPTZ '{_ts_bound(start_date)}'"
    )

    for prefix in prefixes:
        prefix_sql = _sql_quote(prefix)
        logging.info(f"Filters as SQL: {filters_sql}")
        query = f"""
            SELECT
                assets -> 'data' ->> 'href' AS data_href
            FROM read_parquet('{prefix_sql}', union_by_name=true)
            WHERE ST_Intersects(
                geometry,
                ST_GeomFromGeoJSON('{geojson_sql}')
            ) AND {temporal_sql} AND {filters_sql}
        """
        try:
            # .arrow() returns a RecordBatchReader (a Table also iterates batches).
            batches = con.execute(query).arrow()
        except duckdb.IOException:
            logging.debug(f"No parquet files matched under {prefix}, skipping.")
            continue
        count = 0
        for record_batch in batches:
            hrefs = record_batch.column("data_href").to_pylist()
            count += len(hrefs)
            yield from (href for href in hrefs if href.endswith(asset_type))
        logging.info(f"Prefix: {prefix} items found: {count}")


def _iter_rustac(
    prefixes: list[str],
    roi: dict,
    datetime_range: str,
    cql2_filter: dict | None,
    asset_type: str,
):
    """Yield data asset hrefs by searching geoparquet prefixes with rustac.

    Uses ``search_to_arrow`` (constant-memory record batches; ``arro3-core``
    is a core dependency). rustac returns the ``assets`` field as a JSON
    string, so it is parsed per item.
    """
    from typing import Any, cast

    import rustac

    client = rustac.DuckdbClient()
    # rustac expects the geometry as a GeoJSON string.
    search_kwargs: dict[str, Any] = {
        "intersects": json.dumps(roi),
        "datetime": datetime_range,
    }
    if cql2_filter is not None:
        search_kwargs["filter"] = cql2_filter

    def _hrefs(assets: Any):
        """Yield matching data asset hrefs from a single item's assets."""
        assets = json.loads(assets) if isinstance(assets, str) else assets
        for asset in assets.values():
            if "data" in (asset.get("roles") or []) and asset["href"].endswith(
                asset_type
            ):
                yield asset["href"]

    for prefix in prefixes:
        count = 0
        try:
            table = cast(Any, client.search_to_arrow(prefix, **search_kwargs))
            if table is None:
                # No parquet files matched this prefix (e.g. empty year/tile).
                logging.info(f"Prefix: {prefix} items found: 0")
                continue
            for batch in table.to_batches():
                for item_json in batch.column("assets").to_pylist():
                    for href in _hrefs(item_json):
                        count += 1
                        yield href
        except Exception as e:
            logging.warning(
                f"rustac search failed for {prefix} ({type(e).__name__}: {e}); "
                "skipping."
            )
            continue
        logging.info(f"Prefix: {prefix} items found: {count}")


# ---------------------------------------------------------------------------
# Unified search
# ---------------------------------------------------------------------------


def search(
    bbox: list[float] | None = None,
    polygon: list[float] | None = None,
    geojson: dict | None = None,
    intersects: dict | None = None,
    start: str | datetime.date | None = None,
    end: str | datetime.date | None = None,
    collection: str = "itslive-granules",
    type: str = "serverless",
    engine: str | None = "duckdb",
    stream: bool = False,
    percent_valid_pixels: int = 1,
    mission: str | None = None,
    min_interval: int | None = None,
    max_interval: int | None = None,
    filters: dict | None = None,
    base_catalog_href: str | None = None,
    **kwargs,
):
    """Unified search for ITS_LIVE velocity pair NetCDF files.

    Geometry (one required, priority ``intersects`` > ``geojson`` >
    ``polygon`` > ``bbox``), dates, mission/interval filters and the
    catalog location are resolved here; results are data asset URLs.

    Args:
        bbox: ``[min_lon, min_lat, max_lon, max_lat]``.
        polygon: Flat ``[lon, lat, ...]`` list or list of ``(lon, lat)`` pairs.
        geojson: GeoJSON geometry or ``Feature`` dict.
        intersects: Raw GeoJSON geometry overriding the above.
        start: Start date (``datetime.date`` or ``YYYY-MM-DD`` string).
            Defaults to ``"2000-01-01"``.
        end: End date. Defaults to ``"2025-12-31"``.
        collection: STAC collection. Only used by ``type="pgstac"``;
            ignored for ``type="serverless"`` (the geoparquet store is
            already collection-scoped).
        type: Search backend:

            - ``"serverless"``: query partitioned STAC geoparquet directly
              from S3. Default catalog:
              ``s3://its-live-data/test-space/stac/catalog/warehouse``.
            - ``"pgstac"``: STAC API search via ``pystac_client``.
              Default catalog: ``https://stac.itslive.cloud``.

        engine: ``"duckdb"`` or ``"rustac"``, only used with
            ``type="serverless"``. Ignored for ``type="pgstac"``.
        stream: When True, return a generator yielding hrefs as they are
            found (suitable for 1M+ results). When False (default), return
            a sorted, deduplicated list.
        percent_valid_pixels: Minimum percent of valid pixels.
        mission: Satellite mission (``"landsatOLI"``, ``"sentinel1"``,
            ``"sentinel2"``); translated to a ``platform`` filter.
        min_interval: Minimum time separation in days (``date_dt`` filter).
        max_interval: Maximum time separation in days.
        filters: Dict of property filters as ``{property: PropertyFilter}``;
            overrides the parameter-based filters. Use the ``EQ``, ``GTE``,
            ``LTE``, ``GT``, ``LT``, ``NEQ`` helpers.
        base_catalog_href: Explicit catalog location. Defaults depend on
            ``type`` (see above).
        **kwargs: Backend-specific options — ``partition_type``
            (``"h3"``/``"latlon"``), ``resolution``, ``overlap``,
            ``reduce_spatial_search``, ``use_hive_partitions``,
            ``use_year_partitions``, ``asset_type`` (default ``".nc"``).

    Returns:
        list[str] | Iterator[str]: Data asset URLs, sorted and deduplicated,
            or a generator when ``stream=True``.
    """
    search_type = (type or "serverless").lower()
    if search_type == "pgstac":
        if engine not in (None, "duckdb"):
            raise ValueError(f"engine={engine!r} is not valid with type='pgstac'.")
        backend = None
    elif search_type == "serverless":
        backend = (engine or "duckdb").lower()
        if backend not in ("duckdb", "rustac"):
            raise ValueError(
                f"Invalid engine: {engine!r}. Must be 'duckdb' or 'rustac'."
            )
    else:
        raise ValueError(f"Invalid type: {type!r}. Must be 'serverless' or 'pgstac'.")

    roi = intersects if intersects is not None else build_roi(bbox, polygon, geojson)
    start_date = _norm_date(start, DEFAULT_START_DATE)
    end_date = _norm_date(end, DEFAULT_END_DATE)
    datetime_range = f"{start_date}/{end_date}"

    final_filters, extra_cql2_exprs = build_search_filters(
        percent_valid_pixels=percent_valid_pixels,
        mission=mission,
        min_interval=min_interval,
        max_interval=max_interval,
        filters=filters,
    )
    cql2_filter_list = build_cql2_filters_from_dict(final_filters) + extra_cql2_exprs
    cql2_filter = build_cql2_filter(cql2_filter_list)

    asset_type = kwargs.pop("asset_type", ".nc")
    limit = kwargs.pop("limit", 10000)
    logging.info(f"Search filters: {datetime_range} {final_filters} {extra_cql2_exprs}")

    if search_type == "pgstac":
        store = base_catalog_href or DEFAULT_STAC_API_HREF
        # Some pgstac deployments reject bare dates; send full UTC timestamps
        # and make the end bound inclusive.
        pgstac_datetime = f"{_ts_bound(start_date)}/{_ts_bound(end_date, end=True)}"
        hrefs = _iter_pgstac(
            store, collection, roi, pgstac_datetime, cql2_filter, asset_type, limit
        )
    else:
        store, opts = _resolve_serverless_opts(base_catalog_href, **kwargs)
        prefixes = _build_search_prefixes(store, roi, opts, start_date, end_date)
        logging.info(f"Searching in {prefixes}")
        if backend == "duckdb":
            filters_sql = filters_to_where(cql2_filter_list) or "TRUE"
            hrefs = _iter_duckdb(
                prefixes, roi, start_date, end_date, filters_sql, asset_type
            )
        else:
            hrefs = _iter_rustac(prefixes, roi, datetime_range, cql2_filter, asset_type)

    if stream:
        return hrefs
    return sorted(set(hrefs))


def serverless_search(
    start_date: str,
    end_date: str,
    roi: dict,
    filters: dict | None = None,
    base_catalog_href: str | None = None,
    engine: str = "duckdb",
    collection: str | None = None,
    **kwargs,
):
    """Deprecated: use :func:`search` with ``type="serverless"``.

    Kept for backwards compatibility; forwards all arguments to
    ``search(intersects=roi, type="serverless", ...)``.
    """
    import warnings

    warnings.warn(
        "serverless_search is deprecated; use "
        "itslive.search(type='serverless', engine=..., intersects=...).",
        DeprecationWarning,
        stacklevel=2,
    )
    return search(
        intersects=roi,
        start=start_date,
        end=end_date,
        collection=collection or "itslive-granules",
        type="serverless",
        engine=engine,
        filters=filters,
        base_catalog_href=base_catalog_href,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------


def transform_coord(
    proj1: str, proj2: str, lon: float, lat: float
) -> tuple[float, float]:
    """Transform coordinates from proj1 to proj2 (EPSG num)."""
    transformer = pyproj.Transformer.from_crs(
        f"EPSG:{proj1}", f"EPSG:{proj2}", always_xy=True
    )
    return transformer.transform(lon, lat)


#
# Author: Mark Fahnestock
#
def point_to_prefix(lat: float, lon: float, dir_path: str | None = None) -> str:
    """
    Returns a string (for example, N78W124) for directory name based on
    granule centerpoint lat,lon
    """
    nshemi_str = "N" if lat >= 0.0 else "S"
    ewhemi_str = "E" if lon >= 0.0 else "W"

    outlat = int(10 * np.trunc(np.abs(lat / 10.0)))
    if outlat == 90:
        outlat = 80

    outlon = int(10 * np.trunc(np.abs(lon / 10.0)))

    if outlon >= 180:
        outlon = 170

    dirstring = f"{nshemi_str}{outlat:02d}{ewhemi_str}{outlon:03d}"
    if dir_path is not None:
        dirstring = os.path.join(dir_path, dirstring)

    return dirstring
