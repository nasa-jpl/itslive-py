# to get and use geojson datacube catalog
# for timing data access
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import numpy as np
import pyproj
# for datacube xarray/zarr access
import xarray as xr
from rich import print as rprint
from rich.progress import track
from shapely import geometry

from itslive.dataviz import plot_terminal
from itslive.search import serverless_search


# class to throw time series lookup errors
class timeseriesException(Exception):
    pass


# STAC catalog configuration
STAC_CATALOG_URL = "https://stac.itslive.cloud/"
STAC_COLLECTION = "itslive-cubes"


# keep track of open cubes so that we don't re-open (it is slow to re-read xarray metadata
# and dimension vectors)
_open_cubes = {}


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


def _merge_default_variables(variables: List[str]) -> set[str]:
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


def _merge_default_composite_variables(variables: List[str]) -> set[str]:
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
    points: List[tuple[float, float]],
) -> List[Dict[str, Any]]:
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
    cubes: List = []
    if len(points) == 1:
        point = points[0]
        cubes = find_by_point(lon=point[0], lat=point[1])
    if len(points) > 1:
        cubes = find_by_polygon(points)
    return cubes


def find_by_bbox(
    lower_left_lon: float,
    lower_left_lat: float,
    upper_right_lon: float,
    upper_right_lat: float,
) -> List[Dict[str, Any]]:
    """
    Finds the zarr cubes that intersect with a given bounding box
    and returns a list of URLs.

    :param lower_left_lon: lower left longitude
    :param lower_left_lat: lower left latitude
    :param upper_right_lon: upper right longitude
    :param upper_right_lat: upper right latitude
    :returns: list of URLs for the matching Zarr cubes.
    """
    from shapely.geometry import box, mapping

    box_geom = mapping(
        box(lower_left_lon, lower_left_lat, upper_right_lon, upper_right_lat)
    )

    stac_params = {
        "start_date": "2000-01-01",
        "end_date": "2025-12-31",
        "roi": box_geom,
        "collection": STAC_COLLECTION,
        "engine": "stac",
        "base_catalog_href": STAC_CATALOG_URL,
        "asset_type": ".zarr",
        "filters": {},
    }

    try:
        zarr_urls = serverless_search(**stac_params)

        cubes = []
        for url in zarr_urls:
            cubes.append(
                {
                    "type": "Feature",
                    "geometry": box_geom,
                    "properties": {
                        "zarr_url": url,
                        "composite_zarr_url": (
                            url.replace(".zarr", "_composite.zarr")
                            if "_composite" not in url
                            else ""
                        ),
                        "epsg": "3413",
                        "geometry_epsg": box_geom,
                    },
                }
            )

        return cubes
    except Exception as e:
        logging.error(f"Error searching STAC catalog: {e}")
        return []


def find_by_point(lon: float, lat: float) -> List[Dict[str, Any]]:
    """
    Finds the zarr cubes that contain a given lon, lat pair.

    :param lon: longitude
    :param lat: latitude
    :returns: geojson dictionary with matching cube
    """
    from shapely.geometry import mapping

    point_geom = mapping(geometry.Point(lon, lat))

    stac_params = {
        "start_date": "2000-01-01",
        "end_date": "2025-12-31",
        "roi": point_geom,
        "collection": STAC_COLLECTION,
        "engine": "stac",
        "base_catalog_href": STAC_CATALOG_URL,
        "asset_type": ".zarr",
        "filters": {},
    }

    try:
        zarr_urls = serverless_search(**stac_params)

        cubes = []
        for url in zarr_urls:
            cubes.append(
                {
                    "type": "Feature",
                    "geometry": point_geom,
                    "properties": {
                        "zarr_url": url,
                        "composite_zarr_url": (
                            url.replace(".zarr", "_composite.zarr")
                            if "_composite" not in url
                            else ""
                        ),
                        "epsg": "3413",
                        "geometry_epsg": point_geom,
                    },
                }
            )

        return cubes
    except Exception as e:
        logging.error(f"Error searching STAC catalog: {e}")
        return []


def find_by_polygon(points: List[tuple[float, float]] = []) -> List[Dict[str, Any]]:
    """
    Finds the zarr cubes that contain a given polygon.

    :param points: list of polygon points i.e. [(20.1,80.0),(21.1,81.1), ...]
    :returns: list of URLs for the matching Zarr cubes
    """
    from shapely.geometry import Polygon, mapping

    polygon_geom = mapping(Polygon(points))

    stac_params = {
        "start_date": "2000-01-01",
        "end_date": "2025-12-31",
        "roi": polygon_geom,
        "collection": STAC_COLLECTION,
        "engine": "stac",
        "base_catalog_href": STAC_CATALOG_URL,
        "asset_type": ".zarr",
        "filters": {},
    }

    try:
        zarr_urls = serverless_search(**stac_params)

        cubes = []
        for url in zarr_urls:
            cubes.append(
                {
                    "type": "Feature",
                    "geometry": polygon_geom,
                    "properties": {
                        "zarr_url": url,
                        "composite_zarr_url": (
                            url.replace(".zarr", "_composite.zarr")
                            if "_composite" not in url
                            else ""
                        ),
                        "epsg": "3413",
                        "geometry_epsg": polygon_geom,
                    },
                }
            )

        return cubes
    except Exception as e:
        logging.error(f"Error searching STAC catalog: {e}")
        return []


def get_time_series(
    points: List[tuple[float, float]], variables: List[str] = ["v"]
) -> List[Dict[str, Any]]:
    """
    For the points in the list, returns a list of dictionaries - each one containing:
        an xarray DataArray (time series) for each variable on the list for each of the lon lat points.

    :params points: List of (lon, lat) coordinates (EPSG:4326) (e.g. points along the center line of a glacier)
    :params variables: list of variables to be included in the Dataset: v, vx, vy etc.
    :returns: list of dictionaries with coordinates and xarray time series Datasets for the nearest neighbors to the points
                ITS_LIVE processes on a 120 m grid, so nearest points will be close to requested points
    """
    velocity_ts: List = []
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
            # if we have already opened this cube, don't open it again
            if cube_url in _open_cubes:
                xr_da = _open_cubes[cube_url]
            else:
                import s3fs

                fs = s3fs.S3FileSystem(anon=True)
                mapper = fs.get_mapper(cube_s3_url.replace("https://", "s3://"))
                xr_da = xr.open_zarr(mapper, decode_timedelta=True)
                _open_cubes[cube_s3_url] = xr_da
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
    points: List[tuple[float, float]], variables: List[str] = ["v"]
) -> List[Dict[str, Any]]:
    """
    For the points in the list, returns annual composite velocity time series.

    :params points: List of (lon, lat) coordinates (EPSG:4326)
    :params variables: list of variables to be included: v, vx, vy etc.
    :returns: list of dictionaries with coordinates and xarray time series
              Datasets from annual composites
    """
    velocity_ts: List = []
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

            # Use cached dataset or open new one
            if composite_url_https in _open_cubes:
                xr_da = _open_cubes[composite_url_https]
            else:
                import s3fs

                fs = s3fs.S3FileSystem(anon=True)
                mapper = fs.get_mapper(composite_s3_url.replace("https://", "s3://"))
                xr_da = xr.open_zarr(mapper, decode_timedelta=True)
                _open_cubes[composite_s3_url] = xr_da

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
    points: List[tuple[float, float]],
    variables: List[str] = ["v"],
    outdir: Optional[str] = None,
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


def export_netcdf(
    points: List[tuple[float, float]],
    variables: List[str] = ["v"],
    outdir: Optional[str] = None,
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
    points: List[tuple[float, float]],
    variables: List[str] = ["v"],
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


def plot_time_series(
    points: List[tuple[float, float]],
    variable: str = "v",
    label_by: str = "location",
    outdir: Optional[str] = None,
) -> Any:
    return None


def plot_time_series_terminal(
    points: List[tuple[float, float]],
    variable: List[str] = ["v"],
    label_by: str = "location",
    outdir: Optional[str] = None,
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
