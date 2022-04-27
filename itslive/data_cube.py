# to get and use geojson datacube catalog
import json
# for timing data access
import time
from typing import List

import numpy as np
import pyproj
import requests
import s3fs as s3
# for datacube xarray/zarr access
import xarray as xr
from shapely import geometry


# class to throw time series lookup errors
class timeseriesException(Exception):
    pass


class Cube:
    """
    Class to encaptusale discovery and interaction with the ITS_LIVE glacier velocity data cubes.
    """

    def __init__(self):
        CATALOG_URL = (
            "https://its-live-data.s3.amazonaws.com/datacubes/catalog_v02.json"
        )
        # API_ENDPOINT = "https://nsidc.org/apps/itslive-search/velocities/"
        response = requests.get(CATALOG_URL)
        self.catalog = response.json()
        self._s3fs = s3.S3FileSystem(anon=True)
        # keep track of open cubes so that we don't re-read xarray metadata and dimension vectors
        self.open_cubes = {}

    def find_bbox(
        self,
        lower_left_lon: float,
        lower_left_lat: float,
        upper_right_lon: float,
        upper_right_lat: float,
    ) -> List[str]:
        """
        Finds the zarr cubes that intersect with a given bounding box
        and returns a list of URLs.

        :param lower_left_lon: lower left longitude
        :param lower_left_lat: lower left longitude
        :param upper_right_lon: lower left longitude
        :param upper_right_lat: lower left longitude
        :returns: list of URLs for the matching Zarr cubes.
        """
        return [""]

    def find_point(self, lon: float, lat: float) -> List[str]:
        """
        Finds the zarr cubes that contain a given lon, lat pair.

        :param lon: longitude
        "param lat: latitude
        :returns: list of URLs for the matching Zarr cubes
        """
        return [""]

    def find_polygon(self, points: List[float] = []) -> List[str]:
        """
        Finds the zarr cubes that contain a given polygon.

        :param points: list of polygon points
        :returns: list of URLs for the matching Zarr cubes
        """
        return [""]

    def get_time_series(self, points: List[float] = [], variables: List[str] = []):
        """
        Returns an xarray DataArray (time series) for each variable on the list for each of the lon lat points.

        :params points: List of lon lat coordinates (i.e. some points laong the center line of a glacier)
        :params variables: list of variables included in the DataArray: v, vx, vy etc.
        """
        return None

    def subset(self, geom: List[float]):
        """
        Create a subsettted cube on the fly.
        """
        return None
