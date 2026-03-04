import collections
import functools
import json
import logging
import math
import numpy as np
import os
import pyproj
import random
import time
from shapely.geometry import shape, box
import s3fs


def timing_decorator(func):
    """Decorator to time function execution.

    Args:
        func: Function to invoke.
    """

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(
            f"Function {func.__name__}() executed in {elapsed_time:.6f} "
            f"seconds ({elapsed_time / 60:.6f} minutes)"
        )
        return result

    return wrapper


def retry_decorator(max_retries=3, base_delay=1.0, backoff=2.0, jitter=True):
    """
    Decorator to retry a function on any exception.

    Args:
        max_retries (int): Number of retry attempts.
        base_delay (float): Initial delay between retries.
        backoff (float): Backoff multiplier between retries.
        jitter (bool): Whether to add random jitter to the delay.

    Usage:
        @retry(max_retries=3)
        def my_func(): ...
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise
                    sleep_time = random.uniform(0, delay) if jitter else delay
                    logging.info(
                        f"[Retry {attempt}] {type(e).__name__}: {e} — "
                        f"retrying in {sleep_time:.2f}s..."
                    )
                    time.sleep(sleep_time)
                    delay *= backoff

        return wrapper

    return decorator


def bucket_cube_name_from_url(source_url: str) -> str:
    """Extract bucket name and file URL from the given datacube URL.

    Args:
        source_url (str): AWS S3 URL of the datacube in Zarr format.

    Returns:
        str: Tuple of bucket name and file URL.
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
    Recreate the two filters that were previously hard-coded in
    ``serverless_search``.  Use this to migrate callers that previously
    relied on the ``epsg_code`` / ``percent_valid_pixels`` parameters.

    Args:
        epsg_code (str): Numeric EPSG code, e.g. ``"3413"``.
        percent_valid_pixels (float): Minimum valid-pixel fraction.

    Returns:
        dict: ``{property_name: PropertyFilter}`` ready to pass as
            ``serverless_search(filters=...)``.

    Example::

        serverless_search(
            start_date="2022-01-01",
            end_date="2022-12-31",
            roi=my_roi,
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
            return f"'{val}'"
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


def path_exists(path: str) -> bool:
    """
    Check whether a local or S3 path exists.
    """
    if path.startswith("s3://"):
        fs = s3fs.S3FileSystem(anon=True)
        return fs.exists(path)
    else:
        return os.path.exists(path)


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

            {base_href}/grid=h3/level={resolution}/tile={hex_id}/**/*.parquet

        When False (default), the legacy integer-prefix scheme is used::

            {base_href}/{int(hex_id, 16)}/**/*.parquet

    Returns
    -------
    List[str]
        S3-style path prefixes (with wildcards) pointing to ``.parquet``
        files under the overlapping spatial partitions.
    """
    if partition_type == "latlon":

        def lat_prefix(lat):
            return f"N{abs(lat):02d}" if lat >= 0 else f"S{abs(lat):02d}"

        def lon_prefix(lon):
            return f"E{abs(lon):03d}" if lon >= 0 else f"W{abs(lon):03d}"

        geom = shape(geojson_geometry)
        missions = ["landsatOLI", "sentinel1", "sentinel2"]

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

        prefixes = [f"{base_href}/{p}/{i}" for p in missions for i in list(grids)]
        search_prefixes = [
            f"{path}/**/*.parquet" for path in prefixes if path_exists(path)
        ]
        return search_prefixes

    elif partition_type == "h3":
        import h3

        grids_hex = h3.h3shape_to_cells_experimental(
            h3.geo_to_h3shape(geojson_geometry), resolution, overlap
        )

        if use_hive_partitions:
            # Hive-partition layout:
            #   {base_href}/grid=h3/level={resolution}/tile={hex_id}/
            prefixes = [
                f"{base_href}/grid=h3/level={resolution}/tile={hex_id}"
                for hex_id in grids_hex
            ]
        else:
            # Legacy layout: hex cell ID converted to integer directory name.
            prefixes = [f"{base_href}/{int(hex_id, 16)}" for hex_id in grids_hex]

        search_prefixes = [
            f"{prefix}/**/*.parquet" for prefix in prefixes if path_exists(prefix)
        ]
        return search_prefixes

    else:
        raise NotImplementedError(f"Partition {partition_type} not implemented.")


@timing_decorator
@retry_decorator()
def serverless_search(
    start_date: str,
    end_date: str,
    roi: dict,
    filters: dict = {},
    base_catalog_href: str = "s3://its-live-data/test-space/stac/geoparquet/h3r1",
    engine: str = "duckdb",
    reduce_spatial_search: bool = True,
    partition_type: str = "h3",
    resolution: int = 1,
    overlap: str = "bbox_overlap",
    asset_type: str = ".nc",
    use_hive_partitions: bool = True,
    collection: str = "itslive-granules",
):
    """
    Performs a serverless search over partitioned STAC catalogs stored in
    Parquet format for the ITS_LIVE project.

    Parameters
    ----------
    start_date : str
        Start date in ISO 8601 format (e.g., ``"2020-01-01"``).
    end_date : str
        End date in ISO 8601 format (e.g., ``"2020-12-31"``).
    roi : dict
        GeoJSON-like dictionary defining the region of interest.
    filters : dict[str, PropertyFilter], optional
        Property filters to apply, expressed as a mapping of STAC property
        name to a ``PropertyFilter(op, value)``.

        Any property present in a STAC item's ``properties`` block is
        valid — not only the two that were previously hard-coded.
        Use the ``EQ``, ``GTE``, ``LTE``, ``GT``, ``LT``, ``NEQ`` helpers
        for readability.  Example::

            from itslive_utils import EQ, GTE

            serverless_search(
                start_date="2022-01-01",
                end_date="2022-12-31",
                roi=my_roi,
                filters={
                    "proj:code":            EQ("EPSG:3413"),
                    "percent_valid_pixels": GTE(85.0),
                    "platform":             EQ("S2B"),
                    "version":              EQ("002"),
                    "updated":              GTE("2025-01-01T00:00:00Z"),
                    "created":              GTE("2025-01-01T00:00:00Z"),
                },
            )

        Defaults to ``None`` (no property filters applied, all items match).

    base_catalog_href : str
        For ``"duckdb"`` and ``"rustac"``: root URI of the geoparquet
        collection (already scoped to the collection).
        For ``"stac"``: root URI of the STAC API
        (e.g. ``"https://stac.its-live.org"``).
    engine : str
        Query backend: ``"duckdb"``, ``"rustac"``, or ``"stac"``.

        ``"stac"`` issues a standard STAC API search against
        ``base_catalog_href`` using ``pystac_client``.  Spatial filtering
        is handled natively by the API, so ``reduce_spatial_search``,
        ``partition_type``, and ``use_hive_partitions`` are ignored.
    reduce_spatial_search : bool
        Pre-filter parquet files to overlapping spatial partitions.
        Ignored when engine is ``"stac"``.
    partition_type : str
        Spatial partitioning scheme: ``"latlon"`` or ``"h3"``.
        Ignored when engine is ``"stac"``.
    resolution : int
        H3 resolution (only used when partition_type == ``"h3"``).
    overlap : str
        Overlap mode for H3 cell lookup.
    asset_type : str
        Suffix filter for asset HREFs (e.g., ``".nc"``).
    use_hive_partitions : bool
        When True, build/interpret paths as Hive-partitioned.
        Ignored when engine is ``"stac"``.
    collection : str
        STAC collection to search.  Only used by the ``"stac"`` engine,
        where it is passed as ``collections=[collection]`` to
        ``pystac_client``.  For ``"duckdb"`` and ``"rustac"``,
        ``base_catalog_href`` already points at the collection's parquet
        root so this parameter is ignored.  Defaults to
        ``"itslive-granules"``.
    epsg_code : str, optional
        **Deprecated.** Use ``filters={"proj:code": EQ(f"EPSG:{epsg_code}")}``
        instead.
    percent_valid_pixels : float, optional
        **Deprecated.** Use ``filters={"percent_valid_pixels": GTE(value)}``
        instead.

    Returns
    -------
    List[str]
        Asset URLs matching the search criteria.
    """
    import duckdb
    import rustac

    # ------------------------------------------------------------------
    # Build CQL2 filter expressions from the generic filters dict.
    # ------------------------------------------------------------------
    cql2_filter_list = build_cql2_filters_from_dict(filters) if filters else []
    filters_sql = filters_to_where(cql2_filter_list) if cql2_filter_list else "TRUE"
    cql2_filter = build_cql2_filter(cql2_filter_list) if cql2_filter_list else None

    store = base_catalog_href

    search_kwargs = {
        "intersects": roi,
        "datetime": f"{start_date}/{end_date}",
    }
    if cql2_filter is not None:
        search_kwargs["filter"] = cql2_filter

    logging.info(f"Search filters: {search_kwargs}")

    # ------------------------------------------------------------------
    # STAC API engine — delegates everything to pystac-client.
    # The API handles spatial/temporal/property filtering natively, so
    # partition pre-filtering is skipped entirely.
    # ------------------------------------------------------------------
    if engine == "stac":
        import pystac_client

        logging.info(f"Querying STAC API at {store}, collection={collection}")

        stac_client = pystac_client.Client.open(store)

        stac_search_kwargs = {
            "intersects": roi,
            "datetime": f"{start_date}/{end_date}",
            "collections": [collection],
        }
        if cql2_filter is not None:
            stac_search_kwargs["filter"] = cql2_filter
            stac_search_kwargs["filter_lang"] = "cql2-json"

        logging.info(f"STAC search kwargs: {stac_search_kwargs}")

        try:
            item_search = stac_client.search(**stac_search_kwargs)
            items = list(item_search.items())
        except Exception:
            logging.debug(f"STAC API at {store} returned no items, skipping.")
            items = []

        hrefs = []
        for item in items:
            for asset in item.assets.values():
                roles = asset.roles or []
                if "data" in roles and asset.href.endswith(asset_type):
                    hrefs.append(asset.href)

        logging.info(f"STAC API items found: {len(items)}")
        return sorted(list(set(hrefs)))

    # ------------------------------------------------------------------
    # Resolve which parquet prefixes to query (duckdb / rustac only).
    # base_catalog_href already points at the collection root for these
    # engines, so no collection scoping is needed here.
    # ------------------------------------------------------------------
    if reduce_spatial_search and "intersects" in search_kwargs:
        search_prefixes = get_overlapping_grid_names(
            base_href=store,
            geojson_geometry=search_kwargs["intersects"],
            partition_type=partition_type,
            resolution=resolution,
            overlap=overlap,
            use_hive_partitions=use_hive_partitions,
        )
    elif partition_type == "latlon":
        search_prefixes = [
            f"{store}/{mission}/**/*.parquet"
            for mission in ["landsatOLI", "sentinel1", "sentinel2"]
        ]
    elif partition_type == "h3" and use_hive_partitions:
        search_prefixes = [f"{store}/grid=h3/level={resolution}/tile=*/**/*.parquet"]
    else:
        search_prefixes = [f"{store}/**/*.parquet"]

    logging.info(f"Searching in {search_prefixes}")

    # ------------------------------------------------------------------
    # Execute queries (duckdb / rustac).
    # ------------------------------------------------------------------
    con = duckdb.connect()
    client = rustac.DuckdbClient()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")

    hrefs = []
    for prefix in search_prefixes:
        if engine == "duckdb":
            logging.info(f"Filters as SQL: {filters_sql}")
            geojson_str = json.dumps(search_kwargs["intersects"])
            query = f"""
                SELECT
                    '{prefix}' AS source_parquet,
                    assets -> 'data' ->> 'href' AS data_href
                FROM read_parquet('{prefix}', union_by_name=true)
                WHERE ST_Intersects(
                    geometry,
                    ST_GeomFromGeoJSON('{geojson_str}')
                ) AND {filters_sql}
            """
            try:
                items = con.execute(query).df()
            except duckdb.IOException:
                # Glob resolved to zero files — tile exists but has no data
                # for this spatial/temporal/filter combination.
                logging.debug(f"No parquet files matched under {prefix}, skipping.")
                continue
            links = items["data_href"].to_list()
            hrefs.extend(links)

        elif engine == "rustac":
            try:
                items = list(client.search(prefix, **search_kwargs))
            except Exception:
                # Prefix exists but yielded no items (empty tile / no coverage).
                logging.debug(f"No items returned for {prefix}, skipping.")
                continue
            for item in items:
                for asset in item["assets"].values():
                    if "data" in asset["roles"] and asset["href"].endswith(asset_type):
                        hrefs.append(asset["href"])

        else:
            raise NotImplementedError(f"Not a valid query engine: {engine}")

        logging.info(f"Prefix: {prefix} items found: {len(items)}")

    return sorted(list(set(hrefs)))


def transform_coord(proj1, proj2, lon, lat):
    """Transform coordinates from proj1 to proj2 (EPSG num)."""
    proj1 = pyproj.Proj("+init=EPSG:" + proj1)
    proj2 = pyproj.Proj("+init=EPSG:" + proj2)
    return pyproj.transform(proj1, proj2, lon, lat)


#
# Author: Mark Fahnestock
#
def point_to_prefix(lat: float, lon: float, dir_path: str = None) -> str:
    """
    Returns a string (for example, N78W124) for directory name based on
    granule centerpoint lat,lon
    """
    NShemi_str = "N" if lat >= 0.0 else "S"
    EWhemi_str = "E" if lon >= 0.0 else "W"

    outlat = int(10 * np.trunc(np.abs(lat / 10.0)))
    if outlat == 90:
        outlat = 80

    outlon = int(10 * np.trunc(np.abs(lon / 10.0)))

    if outlon >= 180:
        outlon = 170

    dirstring = f"{NShemi_str}{outlat:02d}{EWhemi_str}{outlon:03d}"
    if dir_path is not None:
        dirstring = os.path.join(dir_path, dirstring)

    return dirstring
