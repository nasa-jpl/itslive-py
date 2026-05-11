import logging
import tempfile
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import itslive
from itslive import velocity_cubes as cubes

logger = logging.getLogger(__name__)


def test_imports():
    functions = [
        "find",
        "find_by_point",
        "get_time_series",
        "export_parquet",
        "export_csv",
    ]
    from itslive import velocity_cubes

    assert velocity_cubes

    for f in functions:
        assert hasattr(velocity_cubes, f)
        assert callable(getattr(velocity_cubes, f))


def test_export_parquet_creates_file():
    """Verify export_parquet writes a parquet file without network access."""
    times = pd.date_range("2020-01-01", periods=3, freq="YE")
    ds = xr.Dataset(
        {
            "v": xr.DataArray([100.0, 200.0, 300.0], dims=["mid_date"]),
            "v_error": xr.DataArray([10.0, 20.0, 30.0], dims=["mid_date"]),
            "date_dt": xr.DataArray(
                np.array([30, 60, 90], dtype="timedelta64[D]"), dims=["mid_date"]
            ),
            "satellite_img1": xr.DataArray(["1", "2", "1"], dims=["mid_date"]),
            "mission_img1": xr.DataArray(
                ["sentinel1", "sentinel2", "sentinel1"], dims=["mid_date"]
            ),
        },
        coords={"mid_date": times},
    )
    ds.attrs["projection"] = "3413"

    mock_result = {
        "requested_point_geographic_coordinates": (-49.09, 70.0),
        "returned_point_geographic_coordinates": (-49.1, 70.01),
        "time_series": ds,
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        with patch(
            "itslive.velocity_cubes._cubes.get_time_series", return_value=[mock_result]
        ):
            with patch(
                "itslive.velocity_cubes._cubes.track", side_effect=lambda x, **kw: x
            ):
                cubes.export_parquet([(-49.09, 70.0)], variables=["v"], outdir=tmpdir)

        import os

        files = os.listdir(tmpdir)
        assert len(files) == 1
        assert files[0].endswith(".parquet")

        df = pd.read_parquet(os.path.join(tmpdir, files[0]))
        assert "lon" in df.columns
        assert "lat" in df.columns
        assert "v [m/yr]" in df.columns
        assert "epsg" in df.columns
        assert len(df) == 3
        assert df["epsg"].iloc[0] == "3413"


def test_we_can_verify_version():
    assert type(itslive.__version__) is str


def test_module_has_stac_constants():
    assert cubes.STAC_CATALOG_URL == "https://stac.itslive.cloud"
    assert cubes.STAC_COLLECTION == "itslive-cubes"


@pytest.mark.integration
def test_find_cubes_stac_api_connectivity():
    """Verify we can connect to the STAC API and retrieve cubes."""
    results = cubes.find_by_point(lon=-45.1, lat=75.0)
    assert type(results) is list
    for cube in results:
        assert "properties" in cube
        assert "zarr_url" in cube["properties"]
        assert cube["properties"]["zarr_url"].endswith(".zarr")
        assert "epsg" in cube["properties"]


@pytest.mark.integration
@pytest.mark.parametrize(
    "lon, lat",
    [
        (-45.1, 75.0),
        (-10.0, -76.1),
    ],
)
def test_find_cubes_with_valid_coordinates(lon, lat):
    results = cubes.find_by_point(lon=lon, lat=lat)
    assert len(results) >= 1
    cube = results[0]
    assert cube["properties"]["zarr_url"].endswith(".zarr")


@pytest.mark.integration
@pytest.mark.parametrize(
    "lon, lat",
    [
        (-1275.0, -145.1),
        (0.0, 0.0),
        (33.5, 190.2),
    ],
)
def test_find_cubes_with_invalid_coordinates(lon, lat):
    results = cubes.find_by_point(lon=lon, lat=lat)
    assert type(results) is list
    assert len(results) == 0


@pytest.mark.integration
def test_get_velocity_time_series_for_a_single_point():
    lat = 70.0
    lon = -49.09
    points = [(lon, lat)]
    ts = cubes.get_time_series(points, variables=["v"])
    assert type(ts) is list
    assert len(ts) > 0
    assert "requested_point_geographic_coordinates" in ts[0]
    assert "returned_point_geographic_coordinates" in ts[0]
    assert ts[0]["requested_point_geographic_coordinates"] == (lon, lat)
    assert type(ts[0]["time_series"]) is xr.Dataset
    assert type(ts[0]["time_series"].v) is xr.DataArray


@pytest.mark.integration
def test_get_velocity_time_series_antarctic_coordinates():
    """Issue #9 regression test — verify Antarctic coordinates return data."""
    lon = -63.81
    lat = -65.60
    points = [(lon, lat)]
    ts = cubes.get_time_series(points, variables=["v"])
    assert type(ts) is list
    assert len(ts) > 0, f"No data returned for Antarctic coordinates ({lon}, {lat})"
    assert "time_series" in ts[0]
    assert type(ts[0]["time_series"]) is xr.Dataset
    assert "v" in ts[0]["time_series"]
    assert len(ts[0]["time_series"]["v"]) > 0
