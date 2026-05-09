import pytest
from shapely import geometry

from itslive.velocity_cubes._cubes import (
    _datacube_to_composite_url,
    _get_geographic_point_from_projected,
    _get_projected_xy_point,
    _merge_default_composite_variables,
    _merge_default_variables,
    list_variables,
)


class TestGetProjectedXYPoint:
    def test_returns_point_object(self):
        pt = _get_projected_xy_point(-45.0, 75.0, "3413")
        assert isinstance(pt, geometry.Point)
        assert pt.x != -45.0
        assert pt.y != 75.0

    def test_projection_changes_coordinates(self):
        pt = _get_projected_xy_point(0.0, 0.0, "3413")
        assert abs(pt.x) > 0 or abs(pt.y) > 0


class TestGetGeographicPointFromProjected:
    def test_returns_point_object(self):
        pt = _get_geographic_point_from_projected(0.0, 0.0, "3413")
        assert isinstance(pt, geometry.Point)

    def test_roundtrip_with_projected_xy(self):
        lon, lat = -45.0, 75.0
        projected = _get_projected_xy_point(lon, lat, "3413")
        geographic = _get_geographic_point_from_projected(
            projected.x, projected.y, "3413"
        )
        assert abs(geographic.x - lon) < 0.1
        assert abs(geographic.y - lat) < 0.1


class TestDatacubeToCompositeUrl:
    def test_basic_conversion(self):
        datacube = (
            "https://its-live-data.s3.amazonaws.com/datacubes/"
            "v2-updated-october2024/N70W040/"
            "ITS_LIVE_vel_EPSG3413_G0120_X-50000_Y-1650000.zarr"
        )
        composite = _datacube_to_composite_url(datacube)
        assert composite.startswith("https://its-live-data.s3.amazonaws.com/composites/annual/")
        assert "N70W040" in composite
        assert "ITS_LIVE_velocity_EPSG3413_120m_X-50000_Y-1650000.zarr" in composite
        assert "v2-updated-september2025" in composite

    def test_returns_empty_for_unmatched_url(self):
        assert _datacube_to_composite_url("https://example.com/not-a-cube.zarr") == ""

    def test_returns_empty_for_bad_format(self):
        assert _datacube_to_composite_url("not-a-url") == ""


class TestMergeDefaultVariables:
    def test_defaults_are_included(self):
        result = _merge_default_variables(["v"])
        assert "v" in result
        assert "v_error" in result
        assert "vx" in result

    def test_custom_variables_merged(self):
        result = _merge_default_variables(["extra_var"])
        assert "extra_var" in result
        assert "v" in result

    def test_no_duplicates(self):
        result = _merge_default_variables(["v", "v"])
        assert len(result) == len(_merge_default_variables([]))


class TestMergeDefaultCompositeVariables:
    def test_defaults_are_included(self):
        result = _merge_default_composite_variables(["v"])
        assert "v" in result
        assert "v0" in result
        assert "dv_dt" in result
        assert "landice" in result

    def test_custom_variables_merged(self):
        result = _merge_default_composite_variables(["custom"])
        assert "custom" in result


class TestListVariables:
    def test_runs_without_error(self):
        list_variables()
