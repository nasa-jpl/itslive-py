import datetime
import logging
import os
import pathlib
import sys
from typing import Any, List, Union

import earthaccess
import requests
from pqdm.threads import pqdm

from itslive.search import EQ, GTE, LTE, serverless_search


def find(
    bbox: Union[List[float], None] = None,
    polygon: Union[List[float], None] = None,
    geojson: Union[dict, None] = None,
    percent_valid_pixels: int = 1,
    mission: Union[None, str] = None,
    start: Union[None, datetime.date] = None,
    end: Union[None, datetime.date] = None,
    min_interval: Union[None, int] = None,
    max_interval: Union[None, int] = None,
    engine: str = "stac",
    filters: dict = None,
    **stac_kwargs,
) -> List[str]:
    """Returns a list velocity netcdf files based on the provided parameters

    Args:
        bbox: List of [min_lon, min_lat, max_lon, max_lat]
        polygon: List of (lon, lat) tuples defining a polygon
        geojson: A GeoJSON geometry dict (e.g. {"type": "Polygon", "coordinates": [...]})
        percent_valid_pixels: Minimum percent of valid pixels
        mission: Satellite mission filter (e.g., "landsatOLI", "sentinel1", "sentinel2")
        start: Start date
        end: End date
        min_interval: Minimum time interval in days
        max_interval: Maximum time interval in days
        engine: Query backend:
            - "stac": STAC API (default)
              Catalog: https://stac.its-live.org
            - "duckdb": geoparquet with duckdb
              Catalog: Must specify via base_catalog_href or partition_type+resolution
            - "rustac": geoparquet with rustac
              Catalog: Must specify via base_catalog_href or partition_type+resolution
        filters: Dict of property filters as {property_name: PropertyFilter}.
                 Use helpers: EQ(), GTE(), LTE(), GT(), LT(), NEQ().
                 Examples: {"platform": EQ("S2"), "version": EQ("002")}
                 If provided, these override the parameter-based filters.
        stac_kwargs: Additional arguments to pass to serverless_search()

    Geoparquet Catalog Paths (for duckdb/rustac engines):
        - H3 partitioning with resolution 2: s3://its-live-data/test-space/stac/geoparquet/h3r2
        - H3 partitioning with resolution 3: s3://its-live-data/test-space/stac/geoparquet/h3r3
        - H3 partitioning with resolution N: s3://its-live-data/test-space/stac/geoparquet/h3rN
        - Lat/lon partitioning: s3://its-live-data/test-space/stac/geoparquet/latlon

    Geoparquet Parameters:
        - partition_type: "h3" (hexagonal) or "latlon" (geographic)
        - resolution: H3 resolution (0-5, higher = smaller cells, default: 2)
        - base_catalog_href: Explicit catalog path (overrides partition_type+resolution)

    Common STAC properties for filtering:
        - platform: "S1", "S2", "L4", "L5", "L7", "L8", "L9"
        - mission: "sentinel1", "sentinel2", "landsatOLI"
        - version: "002", "003"
        - proj:code: "EPSG:3413", "EPSG:3031"
        - percent_valid_pixels: 0-100
        - created: ISO 8601 datetime
        - updated: ISO 8601 datetime

    Returns:
        List of URLs for matching velocity pair NetCDF files
    """
    return list(
        find_streaming(
            bbox=bbox,
            polygon=polygon,
            geojson=geojson,
            percent_valid_pixels=percent_valid_pixels,
            mission=mission,
            start=start,
            end=end,
            min_interval=min_interval,
            max_interval=max_interval,
            engine=engine,
            filters=filters,
            **stac_kwargs,
        )
    )


def find_streaming(
    bbox: Union[List[float], None] = None,
    polygon: Union[List[float], None] = None,
    geojson: Union[dict, None] = None,
    percent_valid_pixels: int = 1,
    mission: Union[None, str] = None,
    start: Union[None, datetime.date] = None,
    end: Union[None, datetime.date] = None,
    min_interval: Union[None, int] = None,
    max_interval: Union[None, int] = None,
    engine: str = "stac",
    filters: dict = None,
    **stac_kwargs,
) -> List[str]:
    """Yields velocity netcdf file URLs one at a time to avoid loading all into memory

    This is a streaming version of find() that yields URLs as they are found,
    making it suitable for processing large result sets (e.g., 1M+ URLs).

    Args:
        bbox: List of [min_lon, min_lat, max_lon, max_lat]
        polygon: List of (lon, lat) tuples defining a polygon
        geojson: A GeoJSON geometry dict (e.g. {"type": "Polygon", "coordinates": [...]})
        percent_valid_pixels: Minimum percent of valid pixels
        mission: Satellite mission filter (e.g., "landsatOLI", "sentinel1", "sentinel2")
        start: Start date
        end: End date
        min_interval: Minimum time interval in days
        max_interval: Maximum time interval in days
        engine: Query backend:
            - "stac": STAC API (default)
              Catalog: https://stac.its-live.org
            - "duckdb": geoparquet with duckdb
              Catalog: Must specify via base_catalog_href or partition_type+resolution
            - "rustac": geoparquet with rustac
              Catalog: Must specify via base_catalog_href or partition_type+resolution
        filters: Dict of property filters as {property_name: PropertyFilter}.
                 Use helpers: EQ(), GTE(), LTE(), GT(), LT(), NEQ().
                 Examples: {"platform": EQ("S2"), "version": EQ("002")}
                 If provided, these override the parameter-based filters.

    Yields:
        URLs for matching velocity pair NetCDF files, one at a time
    """
    from shapely.geometry import Polygon, box, mapping, shape

    if geojson is None and polygon is None and bbox is None:
        print("Search needs a bbox, polygon, or geojson geometry", file=sys.stderr)
        return

    # Build geometry — geojson takes priority, then polygon, then bbox
    if geojson is not None:
        # Accept a full GeoJSON Feature or a bare geometry dict
        if geojson.get("type") == "Feature":
            roi = geojson["geometry"]
        else:
            roi = geojson
        # Validate it is a recognised geometry type
        try:
            shape(roi)  # raises if invalid
        except Exception as e:
            print(f"Invalid GeoJSON geometry: {e}", file=sys.stderr)
            return
    elif polygon is not None:
        # Accept either a flat [lon, lat, lon, lat, ...] list (as produced
        # by the CLI) or a list of (lon, lat) tuples / pairs.
        if polygon and not isinstance(polygon[0], (list, tuple)):
            it = iter(polygon)
            polygon = list(zip(it, it))
        roi = mapping(Polygon(polygon))
    else:
        roi = mapping(box(bbox[0], bbox[1], bbox[2], bbox[3]))

    # Build date range
    if isinstance(start, datetime.date):
        start_date = start.isoformat()
    elif isinstance(start, str):
        start_date = start
    else:
        start_date = "2000-01-01"

    if isinstance(end, datetime.date):
        end_date = end.isoformat()
    elif isinstance(end, str):
        end_date = end
    else:
        end_date = "2025-12-31"

    # Build filters from parameters
    param_filters = {}
    if percent_valid_pixels > 0:
        param_filters["percent_valid_pixels"] = GTE(percent_valid_pixels)
    if mission:
        mission_to_platform = {
            "sentinel1": "S1A",
            "sentinel2": "S2A",
            "landsatOLI": "L8",
        }
        platform = mission_to_platform.get(mission.lower() if mission else "")
        if platform:
            param_filters["platform"] = EQ(platform)
    if min_interval:
        param_filters["min_interval_days"] = GTE(min_interval)
    if max_interval:
        param_filters["max_interval_days"] = LTE(max_interval)

    # Merge with custom filters (custom filters override parameter-based ones)
    if filters:
        param_filters.update(filters)

    final_filters = param_filters

    # Build base catalog href explicitly based on engine and partitioning
    if "base_catalog_href" not in stac_kwargs:
        if engine == "stac":
            default_catalog = "https://stac.itslive.cloud"
        elif engine in ["duckdb", "rustac"]:
            # Build geoparquet path explicitly based on partition type and resolution
            # Note: Only H3 resolutions r1 and r2 are available
            partition_type = stac_kwargs.get("partition_type", "h3")
            resolution = stac_kwargs.get("resolution", 1)

            # Validate resolution is 1 or 2 for H3
            if partition_type == "h3":
                if resolution not in [1, 2]:
                    raise ValueError(
                        f"Invalid H3 resolution: {resolution}. "
                        "Only resolutions 1 (finer) and 2 (coarser) are available."
                    )
                # H3 hexagonal partitioning: h3r{resolution}
                default_catalog = (
                    f"s3://its-live-data/test-space/stac/geoparquet/h3r{resolution}"
                )
            elif partition_type == "latlon":
                default_catalog = "s3://its-live-data/test-space/stac/geoparquet/latlon"
            else:
                raise ValueError(
                    f"Invalid partition_type: {partition_type}. "
                    "Must be 'h3' or 'latlon'."
                )
        else:
            raise ValueError(
                f"Invalid engine: {engine}. Must be 'stac', 'duckdb', or 'rustac'."
            )

        stac_kwargs["base_catalog_href"] = default_catalog
        # Use Hive-style partitions since parquet files are stored that way
        stac_kwargs["use_hive_partitions"] = True

    # Build STAC/geoparquet parameters
    stac_params = {
        "start_date": start_date,
        "end_date": end_date,
        "roi": roi,
        "collection": stac_kwargs.get("collection", "itslive-granules"),
        "engine": engine,
        "base_catalog_href": stac_kwargs["base_catalog_href"],
        "asset_type": stac_kwargs.get("asset_type", ".nc"),
        "filters": final_filters,
    }

    # Add geoparquet-specific parameters
    if engine in ["duckdb", "rustac"]:
        stac_params.update(
            {
                "reduce_spatial_search": stac_kwargs.get("reduce_spatial_search", True),
                "partition_type": stac_kwargs.get("partition_type", "h3"),
                "resolution": stac_kwargs.get("resolution", 1),
                "overlap": stac_kwargs.get("overlap", "bbox_overlap"),
                "use_hive_partitions": stac_kwargs.get("use_hive_partitions", True),
            }
        )

    # Remove None filters
    stac_params["filters"] = {
        k: v for k, v in stac_params["filters"].items() if v is not None
    }

    catalog_desc = "STAC API" if engine == "stac" else f"geoparquet ({engine} engine)"
    print(
        f"Finding matching velocity pairs using {catalog_desc}... ", file=sys.stderr
    )
    try:
        if engine == "stac":
            # Stream directly from STAC API page-by-page to avoid loading all
            # items into memory before yielding.
            import pystac_client

            from itslive.search import build_cql2_filter, build_cql2_filters_from_dict

            stac_client = pystac_client.Client.open(
                stac_params["base_catalog_href"]
            )
            stac_search_kwargs = {
                "intersects": roi,
                "datetime": f"{start_date}/{end_date}",
                "collections": [stac_params["collection"]],
            }
            cql2_filter_list = (
                build_cql2_filters_from_dict(final_filters) if final_filters else []
            )
            cql2_filter = (
                build_cql2_filter(cql2_filter_list) if cql2_filter_list else None
            )
            if cql2_filter is not None:
                stac_search_kwargs["filter"] = cql2_filter
                stac_search_kwargs["filter_lang"] = "cql2-json"

            item_search = stac_client.search(**stac_search_kwargs)
            asset_type = stac_params.get("asset_type", ".nc")
            count = 0
            for item in item_search.items():
                for asset in item.assets.values():
                    roles = asset.roles or []
                    if "data" in roles and asset.href.endswith(asset_type):
                        count += 1
                        yield asset.href
            print(f"Found {count} pairs", file=sys.stderr)
        else:
            urls = serverless_search(**stac_params)
            print(f"Found {len(urls)} pairs", file=sys.stderr)
            for url in urls:
                yield url
    except Exception as e:
        logging.error(f"Error searching {catalog_desc}: {e}")
        return


def coverage(
    bbox: List[float],
    polygon: List[float],
    percent_valid_pixels: int = 1,
    mission: Union[None, str] = None,
    start: Union[None, datetime.date] = None,
    end: Union[None, datetime.date] = None,
    min_interval: Union[None, int] = None,
    max_interval: Union[None, int] = None,
    engine: str = "stac",
    **stac_kwargs,
) -> List[Any]:
    """Returns a list of velocity files counts by year on a given area

    Note: The legacy coverage API is no longer available. This function now
    returns a placeholder indicating that feature is not yet implemented
    for STAC/geoparquet catalogs.

    To get similar statistics, you can use find() and analyze the results.
    """
    logging.warning(
        "The coverage() function is not yet implemented for STAC/geoparquet catalogs. "
        "Use find() to retrieve granule URLs and analyze them locally."
    )
    return []


def _download_aws(urls: List[str], path: str) -> List[str]:
    # Closure!
    def _download_file_aws(url: str) -> str:
        local_filename = pathlib.Path(path) / pathlib.Path(url.split("/")[-1])
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_filename

    results = pqdm(urls, _download_file_aws, n_jobs=4)
    return results


def _download_nsidc(urls: List[str], path: str) -> List[str]:
    auth = earthaccess.login()
    if auth.auhtenticated:
        results = earthaccess.download(urls, path)
        return results


def download(urls: List[str], path: str, limit: int = 2000) -> List[str]:
    """Download ITS_LIVE velocity pairs using a list of URLs"""
    os.makedirs(path, exist_ok=True)
    if urls[0].startswith("https://its-live-data.s3.amazonaws.com"):
        files = _download_aws(urls, path)
    else:
        files = _download_nsidc(urls, path)
    return files
