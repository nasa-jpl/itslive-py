# to get and use geojson datacube catalog
# for timing data access
from typing import Any, Dict, List

import pyproj
import requests
# for datacube xarray/zarr access
import xarray as xr
from shapely import geometry


# class to throw time series lookup errors
class timeseriesException(Exception):
    pass


# TODO: The find functions are somewhat inneficcient, we are doing a full scan when
# we could be using an r-tree.


CATALOG_URL = "https://its-live-data.s3.amazonaws.com/datacubes/catalog_v02.json"


# keep track of open cubes so that we don't re-read xarray metadata
# and dimension vectors
_open_cubes = {}
_catalog = requests.get(CATALOG_URL).json()


def _get_projected_xy_point(lon: float, lat: float, projection: str) -> geometry.Point:
    reprojection = pyproj.Transformer.from_proj(
        "epsg:4326", f"epsg:{projection}", always_xy=True
    )
    point = geometry.Point(*reprojection.transform(lon, lat))
    return point


def find(
    points: List[tuple[float, float]],
) -> List[Dict[str, Any]]:
    """Find geojeson entries matching a geometry, if 2 values are passed
    it will use the point geometry, if 3 or more values are passed it will use
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
        _catalog = requests.get(url).json()
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
            cubes.append(cubefeature)
            if not projected_bbox.contains(projected_point):
                print(
                    "Warning: bbox in projected coordinates does not contain selected point"
                )
                # TODO: implement Mark's fix to find the closest cube
            break

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
            velocity_ts.append({"coordinates": (lon, lat), "time_series": time_series})

    return velocity_ts
