# to get and use geojson datacube catalog
# for timing data access
# for datacube xarray/zarr access
import functools
import logging
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pyproj
import pystac_client
import xarray as xr
from rich import print as rprint
from rich.progress import track
from shapely import geometry

from itslive.dataviz import plot_terminal


class timeseriesException(Exception):  # noqa: N801, N818
    """Raised when a time series lookup fails."""


# STAC catalog configuration
STAC_CATALOG_URL = "https://stac.itslive.cloud"
STAC_COLLECTION = "itslive-cubes"

# Annual composite path configuration
# Composites live under a separate prefix with a different versioning scheme.
# e.g. datacube:  .../datacubes/v2-updated-october2024/{REGION}/ITS_LIVE_vel_{EPSG}_G0120_{XY}.zarr
#      composite: .../composites/annual/v2-updated-september2025/{REGION}/ITS_LIVE_velocity_{EPSG}_120m_{XY}.zarr
COMPOSITE_VERSION = "v2-updated-september2025"


@functools.lru_cache(maxsize=32)
def _open_cached_dataset(url: str) -> xr.Dataset:
    return xr.open_dataset(url, engine="zarr", decode_timedelta=True)


def _get_projected_xy_point(lon: float, lat: float, projection: str) -> geometry.Point:
    reprojection = pyproj.Transformer.from_proj(
        "epsg:4326", f"epsg:{projection}", always_xy=True
    )
    point = geometry.Point(*reprojection.transform(lon, lat))
    return point


def _get_geographic_point_from_projected(
    x: float, y: float, from_projection: str
) -> geometry.Point:
    to_ll_reprojection = pyproj.Transformer.from_proj(
        f"epsg:{from_projection}", "epsg:4326", always_xy=True
    )
    point = geometry.Point(*to_ll_reprojection.transform(x, y))
    return point


def _datacube_to_composite_url(datacube_url: str) -> str:
    """Derive the annual composite zarr URL from a datacube zarr URL.

    Datacube pattern:
        https://its-live-data.s3.amazonaws.com/datacubes/<version>/<REGION>/
            ITS_LIVE_vel_<EPSG>_G0120_<XY>.zarr
    Composite pattern:
        https://its-live-data.s3.amazonaws.com/composites/annual/<COMPOSITE_VERSION>/<REGION>/
            ITS_LIVE_velocity_<EPSG>_120m_<XY>.zarr

    Returns an empty string if the URL does not match the expected datacube pattern.
    """
    import re

    m = re.match(
        r"(https://its-live-data\.s3\.amazonaws\.com/)datacubes/[^/]+/([^/]+)/"
        r"ITS_LIVE_vel_(EPSG\d+)_G0120_(X-?\d+_Y-?\d+)\.zarr",
        datacube_url,
    )
    if not m:
        return ""
    base, region, epsg, xy = m.groups()
    return (
        f"{base}composites/annual/{COMPOSITE_VERSION}/{region}/"
        f"ITS_LIVE_velocity_{epsg}_120m_{xy}.zarr"
    )


def list_variables() -> None:
    """Print the available datacube variables."""
    from rich.table import Table

    table = Table(title="Available ITS_LIVE datacube variables")
    table.add_column("Variable", style="cyan")
    table.add_column("Description")

    _variables = {
        "v": "Ice velocity magnitude [m/yr]",
        "v_error": "Ice velocity magnitude error [m/yr]",
        "vx": "Ice velocity x-component [m/yr]",
        "vx_error": "Ice velocity x-component error [m/yr]",
        "vy": "Ice velocity y-component [m/yr]",
        "vy_error": "Ice velocity y-component error [m/yr]",
        "date_dt": "Time separation between image pair [days]",
        "satellite_img1": "Satellite name for image 1",
        "mission_img1": "Mission name for image 1",
    }
    for var, desc in _variables.items():
        table.add_row(var, desc)

    rprint(table)


def _merge_default_variables(variables: list[str]) -> set[str]:
    _default_variables = [
        "v",
        "v_error",
        "vx",
        "vx_error",
        "vy",
        "vy_error",
        "date_dt",
        "satellite_img1",
        "mission_img1",
    ]
    query_variables = set(_default_variables)
    query_variables.update(variables)
    return query_variables


def _merge_default_composite_variables(variables: list[str]) -> set[str]:
    """Default variables for annual composite datasets."""
    _default_variables = [
        # Time-varying (per year)
        "v",
        "v_error",
        "vx",
        "vx_error",
        "vy",
        "vy_error",
        "count",
        # Climatological (2014-2024 summary)
        "v0",
        "v0_error",
        "vx0",
        "vx0_error",
        "vy0",
        "vy0_error",
        "dv_dt",
        "dvx_dt",
        "dvy_dt",
        "v_amp",
        "v_amp_error",
        "vx_amp",
        "vx_amp_error",
        "vy_amp",
        "vy_amp_error",
        "v_phase",
        "vx_phase",
        "vy_phase",
        "count0",
        "outlier_percent",
        # Masks
        "landice",
        "floatingice",
    ]
    query_variables = set(_default_variables)
    query_variables.update(variables)
    return query_variables


def find(
    points: list[tuple[float, float]],
) -> list[dict[str, Any]]:
    """Find the zarr cube information for a given geometry, if 2 values are passed
    it will use the point geometry, if 3 or more values are passed it will
    search using a polygon.

    :param first:
        Longitude value in 4326 format e.g. 28.1
    :type first: ``float``
    :param second:
        Latitude value in 4326 format e.g. -70.2
    :type second: ``float``
    """
    cubes: list = []
    if len(points) == 1:
        point = points[0]
        cubes = find_by_point(lon=point[0], lat=point[1])
    if len(points) > 1:
        cubes = find_by_polygon(points)
    return cubes


def _search_cubes(
    roi_geom: dict,
    geometry_ref: dict,
) -> list[dict[str, Any]]:
    """Search for Zarr cubes intersecting the given geometry via the STAC API.

    Args:
        roi_geom: JSON-Serializable geometry dict for the STAC query.
        geometry_ref: Geometry dict stored in the result (usually same as roi_geom).

    Returns:
        List of cube feature dicts with properties including zarr_url, epsg, etc.
    """
    try:
        client = pystac_client.Client.open(STAC_CATALOG_URL)

        search = client.search(
            intersects=roi_geom,
            datetime="2000-01-01/2025-12-31",
            collections=[STAC_COLLECTION],
        )

        cubes = []
        for item in search.items():
            zarr_url = None
            for asset in item.assets.values():
                if "data" in (asset.roles or []) and asset.href.endswith(".zarr"):
                    zarr_url = asset.href
                    break

            if not zarr_url:
                continue

            proj_code = item.properties.get("proj:code", "EPSG:3413")
            epsg = proj_code.replace("EPSG:", "") if proj_code else "3413"

            cubes.append(
                {
                    "type": "Feature",
                    "geometry": geometry_ref,
                    "properties": {
                        "zarr_url": zarr_url,
                        "composite_zarr_url": _datacube_to_composite_url(zarr_url),
                        "epsg": epsg,
                        "geometry_epsg": geometry_ref,
                    },
                }
            )

        return cubes
    except Exception as e:
        logging.error(f"Error searching STAC catalog: {e}")
        return []


def find_by_bbox(
    lower_left_lon: float,
    lower_left_lat: float,
    upper_right_lon: float,
    upper_right_lat: float,
) -> list[dict[str, Any]]:
    from shapely.geometry import box, mapping

    roi = mapping(box(lower_left_lon, lower_left_lat, upper_right_lon, upper_right_lat))
    return _search_cubes(roi, roi)


def find_by_point(lon: float, lat: float) -> list[dict[str, Any]]:
    from shapely.geometry import mapping

    roi = mapping(geometry.Point(lon, lat))
    return _search_cubes(roi, roi)


def find_by_polygon(points: list[tuple[float, float]] = []) -> list[dict[str, Any]]:
    from shapely.geometry import Polygon, mapping

    roi = mapping(Polygon(points))
    return _search_cubes(roi, roi)


def get_time_series(
    points: list[tuple[float, float]], variables: list[str] = ["v"]
) -> list[dict[str, Any]]:
    """
    For the points in the list, returns a list of dictionaries - each one containing:
        an xarray DataArray (time series) for each variable on the list for each of the lon lat points.

    :params points: List of (lon, lat) coordinates (EPSG:4326) (e.g. points along the center line of a glacier)
    :params variables: list of variables to be included in the Dataset: v, vx, vy etc.
    :returns: list of dictionaries with coordinates and xarray time series Datasets for the nearest neighbors to the points
                ITS_LIVE processes on a 120 m grid, so nearest points will be close to requested points
    """
    velocity_ts: list = []
    variables = _merge_default_variables(variables)
    for point in points:
        lon = point[0]
        lat = point[1]
        results = find_by_point(lon, lat)
        if len(results):
            cube = results[0]
            projection = cube["properties"]["epsg"]
            zarr_url = cube["properties"]["zarr_url"]
            cube_url = zarr_url.replace("http://", "https://")
            projected_point = _get_projected_xy_point(lon, lat, projection)
            xr_da = _open_cached_dataset(cube_url)
            time_series = xr_da[variables].sel(
                x=projected_point.x, y=projected_point.y, method="nearest"
            )

            if time_series is not None and isinstance(time_series, xr.Dataset):
                x_off = time_series.x.values - projected_point.x
                y_off = time_series.y.values - projected_point.y
                ll_pt = _get_geographic_point_from_projected(
                    time_series.x.values, time_series.y.values, projection
                )
                actual_lon = ll_pt.x
                actual_lat = ll_pt.y

                velocity_ts.append(
                    {
                        "requested_point_geographic_coordinates": (lon, lat),
                        "returned_point_geographic_coordinates": (
                            actual_lon,
                            actual_lat,
                        ),
                        "returned_point_projected_coordinates": {
                            "epsg": projection,
                            "coords": (time_series.x.values, time_series.y.values),
                        },
                        "returned_point_offset_from_requested_in_projection_meters": np.sqrt(
                            x_off**2 + y_off**2
                        ),
                        "time_series": time_series,
                    }
                )

    return velocity_ts


def get_annual_time_series(
    points: list[tuple[float, float]], variables: list[str] = ["v"]
) -> list[dict[str, Any]]:
    """
    For the points in the list, returns annual composite velocity time series.

    :params points: List of (lon, lat) coordinates (EPSG:4326)
    :params variables: list of variables to be included: v, vx, vy etc.
    :returns: list of dictionaries with coordinates and xarray time series
              Datasets from annual composites
    """
    velocity_ts: list = []
    variables = _merge_default_composite_variables(variables)

    for point in points:
        lon = point[0]
        lat = point[1]
        results = find_by_point(lon, lat)

        if len(results):
            cube = results[0]
            properties = cube.get("properties") if isinstance(cube, dict) else None
            if not properties:
                rprint(
                    f"[yellow]No 'properties' found for cube at point ({lon}, {lat}); "
                    "skipping annual composite time series for this point.[/yellow]"
                )
                continue
            composite_url = properties.get("composite_zarr_url")
            if not composite_url:
                rprint(
                    f"[yellow]'composite_zarr_url' not available for cube at point ({lon}, {lat}); "
                    "skipping annual composite time series for this point.[/yellow]"
                )
                continue
            projection = properties["epsg"]
            composite_url_https = composite_url.replace("http://", "https://")
            projected_point = _get_projected_xy_point(lon, lat, projection)

            xr_da = _open_cached_dataset(composite_url_https)

            time_series = xr_da[variables].sel(
                x=projected_point.x, y=projected_point.y, method="nearest"
            )

            if time_series is not None and isinstance(time_series, xr.Dataset):
                x_off = time_series.x.values - projected_point.x
                y_off = time_series.y.values - projected_point.y
                ll_pt = _get_geographic_point_from_projected(
                    time_series.x.values, time_series.y.values, projection
                )

                velocity_ts.append(
                    {
                        "requested_point_geographic_coordinates": (lon, lat),
                        "returned_point_geographic_coordinates": (ll_pt.x, ll_pt.y),
                        "returned_point_projected_coordinates": {
                            "epsg": projection,
                            "coords": (time_series.x.values, time_series.y.values),
                        },
                        "returned_point_offset_from_requested_in_projection_meters": np.sqrt(
                            x_off**2 + y_off**2
                        ),
                        "time_series": time_series,
                    }
                )

    return velocity_ts


def export_csv(
    points: list[tuple[float, float]],
    variables: list[str] = ["v"],
    outdir: str | None = None,
) -> None:
    """Exports a list of ITS_LIVE glacier velocity variables to csv files"""

    query_variables = _merge_default_variables(variables)

    outdir = f"./itslive-{uuid4()}" if outdir is None else outdir

    Path(outdir).mkdir(parents=True, exist_ok=True)

    for point in track(
        points,
        description=f"Processing {len(points)} coordinates...",
        total=len(points),
    ):
        lon = round(point[0], 4)
        lat = round(point[1], 4)
        result_series = get_time_series([(lon, lat)], query_variables)
        if len(result_series):
            series = result_series[0]["time_series"]

            df = series.to_dataframe()
            df["x"] = lon
            df["y"] = lat
            df = df.rename(
                columns={
                    "x": "lon",
                    "y": "lat",
                    "satellite_img1": "satellite",
                    "mission_img1": "mission",
                    "v": "v [m/yr]",
                    "v_error": "v_error [m/yr]",
                    "vx": "vx [m/yr]",
                    "vx_error": "vx_error [m/yr]",
                    "vy": "vy [m/yr]",
                    "vy_error": "vy_error [m/yr]",
                }
            )
            df["epsg"] = series.attrs["projection"]
            df["date_dt [days]"] = df["date_dt"].dt.days
            ts = df.dropna()
            file_name = f"LON{lon}--LAT{lat}.csv"
            ts.to_csv(
                f"{outdir}/{file_name}",
                columns=[
                    "lon",
                    "lat",
                    "v [m/yr]",
                    "v_error [m/yr]",
                    "vx [m/yr]",
                    "vx_error [m/yr]",
                    "vy [m/yr]",
                    "vy_error [m/yr]",
                    "date_dt [days]",
                    "mission",
                    "satellite",
                    "epsg",
                ],
            )
        else:
            rprint(f"[red on black]No data found at[/] lon: {lon}, lat: {lat}")


def export_parquet(
    points: list[tuple[float, float]],
    variables: list[str] = ["v"],
    outdir: str | None = None,
) -> None:
    """Exports a list of ITS_LIVE glacier velocity variables to parquet files."""

    query_variables = _merge_default_variables(variables)

    outdir = f"./itslive-{uuid4()}" if outdir is None else outdir
    Path(outdir).mkdir(parents=True, exist_ok=True)

    for point in track(
        points,
        description=f"Processing {len(points)} coordinates...",
        total=len(points),
    ):
        lon = round(point[0], 4)
        lat = round(point[1], 4)
        result_series = get_time_series([(lon, lat)], query_variables)
        if len(result_series):
            series = result_series[0]["time_series"]

            df = series.to_dataframe()
            df["lon"] = lon
            df["lat"] = lat
            df = df.rename(
                columns={
                    "satellite_img1": "satellite",
                    "mission_img1": "mission",
                    "v": "v [m/yr]",
                    "v_error": "v_error [m/yr]",
                    "vx": "vx [m/yr]",
                    "vx_error": "vx_error [m/yr]",
                    "vy": "vy [m/yr]",
                    "vy_error": "vy_error [m/yr]",
                }
            )
            df["epsg"] = series.attrs["projection"]
            if "date_dt" in df.columns:
                df["date_dt [days]"] = df["date_dt"].dt.days
            ts = df.dropna()
            file_name = f"LON{lon}--LAT{lat}.parquet"
            ts.to_parquet(f"{outdir}/{file_name}")
        else:
            rprint(f"[red on black]No data found at[/] lon: {lon}, lat: {lat}")


def export_netcdf(
    points: list[tuple[float, float]],
    variables: list[str] = ["v"],
    outdir: str | None = None,
) -> None:
    """Exports a list of ITS_LIVE glacier velocity variables to netcdf files"""

    query_variables = _merge_default_variables(variables)

    outdir = f"./itslive-{uuid4()}" if outdir is None else outdir
    Path(outdir).mkdir(parents=True, exist_ok=True)

    for point in track(
        points,
        description=f"Processing {len(points)} coordinates...",
        total=len(points),
    ):
        lon = round(point[0], 4)
        lat = round(point[1], 4)

        file_name = f"LON{lon}--LAT{lat}"
        result_series = get_time_series([(lon, lat)], query_variables)
        if len(result_series):
            series = result_series[0]["time_series"]
            series.to_netcdf(f"{outdir}/{file_name}.nc")
        else:
            rprint(f"[red on black]No data found at[/] lon:{lon}, lat: {lat}")


def export_stdout(
    points: list[tuple[float, float]],
    variables: list[str] = ["v"],
) -> None:
    """Exports a list of ITS_LIVE glacier velocity variables to stdout"""

    query_variables = _merge_default_variables(variables)

    for point in track(
        points,
        description=f"Processing {len(points)} coordinates...",
        total=len(points),
    ):
        lon = round(point[0], 4)
        lat = round(point[1], 4)

        result_series = get_time_series([(lon, lat)], query_variables)
        if len(result_series):
            series = result_series[0]["time_series"]
            df = series.to_dataframe()
            df["x"] = lon
            df["y"] = lat
            df = df.rename(
                columns={
                    "x": "lon",
                    "y": "lat",
                    "satellite_img1": "satellite",
                    "mission_img1": "mission",
                    "v": "v [m/yr]",
                    "v_error": "v_error [m/yr]",
                    "vx": "vx [m/yr]",
                    "vx_error": "vx_error [m/yr]",
                    "vy": "vy [m/yr]",
                    "vy_error": "vy_error [m/yr]",
                }
            )
            df["epsg"] = series.attrs["projection"]
            df["date_dt [days]"] = df["date_dt"].dt.days
            ts = df.dropna()
            outstr = ts.to_markdown()
            rprint(outstr)
        else:
            rprint(f"[red on black] No data found at [/] lon: {lon}, lat: {lat}")


def plot_time_series_terminal(
    points: list[tuple[float, float]],
    variable: list[str] = ["v"],
    label_by: str = "location",
    outdir: str | None = None,
):
    for point in track(
        points,
        description=f"Processing {len(points)} coordinates...",
        total=len(points),
    ):
        lon = round(point[0], 4)
        lat = round(point[1], 4)

        series = get_time_series([(lon, lat)], variables=variable)
        if series is not None and len(series) > 0:
            ts = series[0]["time_series"]
            plot_terminal(lon, lat, ts, variable)
            # Exclude zeros and NaNs when computing meaningful max
            valid = ts[variable].where(ts[variable] > 0)
            max_variable = valid.where(valid == valid.max(), drop=True).squeeze()
            max_value = max_variable[variable[0]].values

            rprint(f"Max {variable} on {max_variable['mid_date'].values}: {max_value}")
            rprint(f"Cube URL: {max_variable.attrs['url']}")
    return None
