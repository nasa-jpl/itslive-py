# to get and use geojson datacube catalog
# for timing data access
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pyproj
import requests
# for datacube xarray/zarr access
import xarray as xr
from rich import print as rprint
from rich.progress import track
from shapely import geometry

from .data_viz import plot_terminal, plot_variable


# class to throw time series lookup errors
class timeseriesException(Exception):
    pass


DEFAULT_CATALOG_URL = (
    "https://its-live-data.s3.amazonaws.com/datacubes/catalog_v02.json"
)


# keep track of open cubes so that we don't re-read xarray metadata
# and dimension vectors
_open_cubes = {}
_catalog = requests.get(DEFAULT_CATALOG_URL).json()
_current_catalog_url = DEFAULT_CATALOG_URL


def _get_projected_xy_point(lon: float, lat: float, projection: str) -> geometry.Point:
    reprojection = pyproj.Transformer.from_proj(
        "epsg:4326", f"epsg:{projection}", always_xy=True
    )
    point = geometry.Point(*reprojection.transform(lon, lat))
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

    return []


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


def load_catalog(
    url: str = "https://its-live-data.s3.amazonaws.com/datacubes/catalog_v02.json",
):
    """Loads a geojson catalog containing all the zarr cube urls and metadata,
    if url is not provided will load the default location on S3
    """
    try:
        _current_catalog_url = url
        _catalog = requests.get(_current_catalog_url).json()
    except Exception:
        raise Exception
    return _catalog


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
    :param lower_left_lat: lower left longitude
    :param upper_right_lon: lower left longitude
    :param upper_right_lat: lower left longitude
    :returns: list of URLs for the matching Zarr cubes.
    """
    cubes: List = []
    box_to_find = geometry.box(
        lower_left_lon, lower_left_lat, upper_right_lon, upper_right_lat
    )
    for f in _catalog["features"]:
        polygeom = geometry.shape(f["geometry"])
        if polygeom.intersects(box_to_find):
            cubes.append(f)
    return cubes



def find_by_point(lon: float, lat: float) -> List[Dict[str, Any]]:
    """
    Finds the zarr cubes that contain a given lon, lat pair.

    :param lon: longitude
    "param lat: latitude
    :returns: geojson dictionary with matching cube
    """
    cubes: List = []
    point = geometry.Point(lon, lat)
    for f in _catalog["features"]:
        polygeom = geometry.shape(f["geometry"])
        if polygeom.contains(point):
            cubefeature = f
            projected_bbox = geometry.shape(cubefeature["properties"]["geometry_epsg"])
            projected_point = _get_projected_xy_point(
                lon, lat, cubefeature["properties"]["epsg"]
            )
            pp_epsg = cubefeature["properties"]["epsg"]
            if not projected_bbox.contains(projected_point):
                print(
                    "bbox in projected coordinates does not contain selected point - searching in local EPSG - ", end = ''
                )
                # re-run all features using cube projection bbox (until you find one, then stop), this time using each cube's epsg bbox for the inclusion test - requires reprojecting point to cube cs
                # TODO- Done Below?: implement Mark's fix to find the closest cube
                found_correct_cube = False
                for g in _catalog["features"]:
                    cubefeature2 = g
                    projected_bbox = geometry.shape(cubefeature2["properties"]["geometry_epsg"])
                    if pp_epsg == cubefeature2["properties"]["epsg"]: # projected point in same epsg as cube bbox
                        point_in_cube_epsg = projected_point
                    else:
                        point_in_cube_epsg = _get_projected_xy_point(
                                                                    lon, lat, cubefeature2["properties"]["epsg"]
                                                                )
                    if projected_bbox.contains(point_in_cube_epsg):
                        cubefeature = cubefeature2
                        print( "found correct cube")
                        found_correct_cube = True
                        break
                if not(found_correct_cube):
                    print(f"correct cube not found \n Warning:  point ({lon},{lat}) does not fall into coverage of available data cubes, nearest cube used here may or may not be appropriate")

            cubes.append(cubefeature)
    if len(cubes)==0:
         print(f"Warning:  point ({lon},{lat}) does not fall into coverage of available data cubes, empty list returned")
         
    return cubes


def find_by_polygon(points: List[tuple[float, float]] = []) -> List[Dict[str, Any]]:
    """
    Finds the zarr cubes that contain a given polygon.

    :param points: list of polygon points i.e. [(20.1,80.0),(21.1,81.1), ...]
    :returns: list of URLs for the matching Zarr cubes
    """
    cubes: List = []
    polygon = geometry.Polygon(points)
    for f in _catalog["features"]:
        polygeom = geometry.shape(f["geometry"])
        if polygeom.intersects(polygon):
            cubes.append(f)
    return cubes


def get_time_series(
    points: List[tuple[float, float]], variables: List[str] = ["v"]
) -> List[Dict[str, Any]]:
    """
    Returns an xarray DataArray (time series) for each variable on the list for each of the lon lat points.

    :params points: List of lon lat coordinates (i.e. some points laong the center line of a glacier)
    :params variables: list of variables included in the Dataset: v, vx, vy etc.
    :returns: list of tuples with coordinates and xarray Datasets for the matching Zarr cubes
    """
    velocity_ts: List = []
    for point in points:
        lon = point[0]
        lat = point[1]
        results = find_by_point(lon, lat)
        if len(results):
            cube = results[0]
            projection = cube["properties"]["epsg"]
            # for zarr store modify URL for use in boto open
            zarr_url = cube["properties"]["zarr_url"]
            cube_s3_url = zarr_url.replace("http:", "s3:").replace(
                ".s3.amazonaws.com", ""
            )
            projected_point = _get_projected_xy_point(lon, lat, projection)
            # if we have already opened this cube, don't open it again
            if len(_open_cubes) and cube_s3_url in _open_cubes.keys():
                xr_da = _open_cubes[cube_s3_url]
            else:
                xr_da = xr.open_dataset(
                    cube_s3_url, engine="zarr", storage_options={"anon": True}
                )
                _open_cubes[cube_s3_url] = xr_da
            time_series = xr_da[variables].sel(
                x=projected_point.x, y=projected_point.y, method="nearest"
            )
            # TODO  - returned coordinates should include the nearest neighbor x and y from the xr.Dataset 
            #       - the lon and lat are the user requested coordinates, not the itslive/xarray nearest neighbor equivalent
            if time_series is not None and isinstance(time_series, xr.Dataset):
                velocity_ts.append(
                    {"coordinates": (lon, lat), "time_series": time_series}
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


def _plot_time_series_terminal(
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
            max_variable = (
                ts[variable]
                .where(ts[variable] == ts[variable].max(), drop=True)
                .squeeze()
            )
            min_variable = (
                ts[variable]
                .where(ts[variable] == ts[variable].min(), drop=True)
                .squeeze()
            )
            max_value = max_variable[variable[0]].values
            min_value = min_variable[variable[0]].values

            rprint(f"Max {variable} on {max_variable['mid_date'].values}: {max_value}")
            rprint(f"Min {variable} on {min_variable['mid_date'].values}: {min_value}")
            rprint(f"Cube URL: {max_variable.attrs['url']}")
    return None
