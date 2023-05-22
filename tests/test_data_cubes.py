import logging

import itslive
import pytest
import xarray as xr
from itslive import velocity_cubes as cubes

logger = logging.getLogger(__name__)

base_url = "http://its-live-data.s3.amazonaws.com/datacubes"

valid_lat_lons = [
    (
        75.0,
        -45.1,
        f"{base_url}/v02/N70W040/ITS_LIVE_vel_EPSG3413_G0120_X-50000_Y-1650000.zarr",
    ),
    (
        -76.1,
        -10.0,
        f"{base_url}/v02/S70W000/ITS_LIVE_vel_EPSG3031_G0120_X-250000_Y1450000.zarr",
    ),
    (
        33.5,
        76.2,
        f"{base_url}/v02/N30E070/ITS_LIVE_vel_EPSG32643_G0120_X650000_Y3750000.zarr",
    ),
]

invalid_lat_lons = [
    (-1275.0, -145.1),
    (0.0, 0.0),
    (33.5, 190.2),
]


def test_imports():
    from itslive import velocity_cubes


def test_we_can_verify_version():
    assert type(itslive.__version__) is str


def test_load_default_cube():
    catalog, url = cubes.load_catalog()
    assert type(catalog) is dict


def test_load_fails_with_unreachable_url():
    with pytest.raises(Exception):
        catalog = cubes.load_catalog("http://something.not.working")
        assert type(catalog) is list
        assert len(catalog) > 0


@pytest.mark.parametrize("lat, lon, expected_url", valid_lat_lons)
def test_find_cubes_with_valid_lat_lons(lat, lon, expected_url):
    results = cubes.find_by_point(lon=lon, lat=lat)
    assert len(results) == 1
    cube = results[0]
    if len(cube):
        assert cube["properties"]["zarr_url"] == expected_url


@pytest.mark.parametrize("lat, lon", invalid_lat_lons)
def test_find_cubes_with_invalid_lat_lons(lat, lon):
    results = cubes.find_by_point(lat=lat, lon=lon)
    assert type(results) is list
    assert len(results) == 0


# This is more of an integration test as it depends on the catalog and
# the cubes being in the right location on S3
def test_get_velocity_time_series_for_a_single_point():
    lat = 70.0
    lon = -49.09
    points = [(lon, lat)]
    ts = cubes.get_time_series(points, variables=["v"])
    if ts is not None:
        assert type(ts) is list
        assert len(ts) > 0
        assert "coordinates" in ts[0]
        assert ts[0]["coordinates"] == (lon, lat)
        assert type(ts[0]["time_series"]) is xr.Dataset
        assert type(ts[0]["time_series"].v) is xr.DataArray
