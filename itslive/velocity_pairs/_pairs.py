import datetime
import logging
import os
import pathlib
import sys
from typing import Any

import earthaccess
import requests
from pqdm.threads import pqdm

from itslive._search import search


def _legacy_engine(engine: str) -> tuple[str, str | None]:
    """Map the legacy ``engine`` parameter to (type, engine)."""
    if engine == "stac":
        return "pgstac", None
    if engine in ("duckdb", "rustac"):
        return "serverless", engine
    raise ValueError(
        f"Invalid engine: {engine}. Must be 'stac', 'duckdb', or 'rustac'."
    )


def find(
    bbox: list[float] | None = None,
    polygon: list | None = None,
    geojson: dict | None = None,
    percent_valid_pixels: int = 1,
    mission: None | str = None,
    start: str | datetime.date | None = None,
    end: str | datetime.date | None = None,
    min_interval: None | int = None,
    max_interval: None | int = None,
    engine: str = "stac",
    filters: dict | None = None,
    **stac_kwargs,
) -> list[str]:
    """Returns a list of velocity netcdf files based on the provided parameters.

    Thin wrapper around :func:`itslive.search` with ``stream=True``; see that
    function for the full list of backend options.

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
        engine: Query backend (legacy parameter):
            - "stac": STAC API (default), mapped to ``type="pgstac"``
              Catalog: https://stac.itslive.cloud
            - "duckdb"/"rustac": geoparquet, mapped to ``type="serverless"``
              Default catalog: s3://its-live-data/test-space/stac/catalog/warehouse
              (pass base_catalog_href for legacy h3r1/h3r2/latlon stores)
        filters: Dict of property filters as {property_name: PropertyFilter}.
                 Use helpers: EQ(), GTE(), LTE(), GT(), LT(), NEQ().
                 If provided, these override the parameter-based filters.
        stac_kwargs: Additional arguments to pass to itslive.search()
            (collection, base_catalog_href, partition_type, resolution, ...)

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
    bbox: list[float] | None = None,
    polygon: list | None = None,
    geojson: dict | None = None,
    percent_valid_pixels: int = 1,
    mission: None | str = None,
    start: str | datetime.date | None = None,
    end: str | datetime.date | None = None,
    min_interval: None | int = None,
    max_interval: None | int = None,
    engine: str = "stac",
    filters: dict | None = None,
    **stac_kwargs,
):
    """Yields velocity netcdf file URLs one at a time to avoid loading all into memory

    Streaming version of find(), suitable for processing large result sets
    (e.g., 1M+ URLs). Thin wrapper around :func:`itslive.search` with
    ``stream=True``.

    Args:
        Same as :func:`find`.

    Yields:
        URLs for matching velocity pair NetCDF files, one at a time
    """
    # Validate eagerly so bad arguments raise at call time, not on first iteration.
    _legacy_engine(engine)
    return _find_streaming_iter(
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


def _find_streaming_iter(
    bbox,
    polygon,
    geojson,
    percent_valid_pixels,
    mission,
    start,
    end,
    min_interval,
    max_interval,
    engine,
    filters,
    **stac_kwargs,
):
    search_type, resolved_engine = _legacy_engine(engine)
    hrefs = search(
        bbox=bbox,
        polygon=polygon,
        geojson=geojson,
        percent_valid_pixels=percent_valid_pixels,
        mission=mission,
        start=start,
        end=end,
        min_interval=min_interval,
        max_interval=max_interval,
        type=search_type,
        engine=resolved_engine,
        stream=True,
        filters=filters,
        **stac_kwargs,
    )
    catalog_desc = (
        "STAC API"
        if search_type == "pgstac"
        else f"geoparquet ({resolved_engine} engine)"
    )
    print(f"Finding matching velocity pairs using {catalog_desc}... ", file=sys.stderr)
    count = 0
    for url in hrefs:
        count += 1
        yield url
    print(f"Found {count} pairs", file=sys.stderr)


def coverage(
    bbox: list[float] | None = None,
    polygon: list | None = None,
    percent_valid_pixels: int = 1,
    mission: None | str = None,
    start: None | datetime.date = None,
    end: None | datetime.date = None,
    min_interval: None | int = None,
    max_interval: None | int = None,
    engine: str = "stac",
    **stac_kwargs,
) -> list[Any]:
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


def _download_aws(urls: list[str], path: str) -> list[str]:
    # Closure!
    def _download_file_aws(url: str) -> str:
        local_filename = pathlib.Path(path) / pathlib.Path(url.split("/")[-1])
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return str(local_filename)

    results = pqdm(urls, _download_file_aws, n_jobs=4)
    return results


def _download_nsidc(urls: list[str], path: str) -> list[str] | None:
    auth = earthaccess.login()
    if auth.authenticated:
        results = earthaccess.download(urls, path)
        return [str(result) for result in results]
    return None


def download(urls: list[str], path: str, limit: int = 2000) -> list[str]:
    """Download ITS_LIVE velocity pairs using a list of URLs"""
    os.makedirs(path, exist_ok=True)
    if urls[0].startswith("https://its-live-data.s3.amazonaws.com"):
        files = _download_aws(urls, path)
    else:
        files = _download_nsidc(urls, path)
    return files or []
