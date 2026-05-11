from unittest.mock import MagicMock, patch

import pytest
from shapely import geometry

from itslive.velocity_cubes._cubes import (
    STAC_CATALOG_URL,
    STAC_COLLECTION,
    _search_cubes,
)

_PATCH_OPEN = "itslive.velocity_cubes._cubes.pystac_client.Client.open"


def _make_mock_item(zarr_url: str, epsg: str = "EPSG:3413"):
    item = MagicMock()
    item.properties = {"proj:code": epsg}
    asset = MagicMock()
    asset.roles = ["data"]
    asset.href = zarr_url
    item.assets = {"data": asset}
    return item


def _mock_stac_open(mock_items: list):
    mock_client = MagicMock()
    mock_search = MagicMock()
    mock_search.items.return_value = mock_items
    mock_client.search.return_value = mock_search
    return patch(_PATCH_OPEN, return_value=mock_client)


class TestSearchCubesUnit:
    """Unit tests for _search_cubes with mocked STAC responses."""

    def test_returns_list_of_cubes(self):
        mock_item = _make_mock_item(
            "https://its-live-data.s3.amazonaws.com/datacubes/v2/N70W040/"
            "ITS_LIVE_vel_EPSG3413_G0120_X-50000_Y-1650000.zarr"
        )
        with _mock_stac_open([mock_item]):
            geom = geometry.mapping(geometry.Point(-45.0, 75.0))
            results = _search_cubes(geom, geom)

        assert len(results) == 1
        cube = results[0]
        assert cube["type"] == "Feature"
        assert "zarr_url" in cube["properties"]
        assert cube["properties"]["epsg"] == "3413"

    def test_stac_client_called_with_correct_args(self):
        with patch(_PATCH_OPEN) as mock_open:
            mock_client = MagicMock()
            mock_search = MagicMock()
            mock_search.items.return_value = []
            mock_client.search.return_value = mock_search
            mock_open.return_value = mock_client

            geom = geometry.mapping(geometry.Point(10.0, 20.0))
            _search_cubes(geom, geom)

            mock_open.assert_called_once_with(STAC_CATALOG_URL)
            mock_client.search.assert_called_once_with(
                intersects=geom,
                datetime="2000-01-01/2025-12-31",
                collections=[STAC_COLLECTION],
            )

    def test_filters_items_without_zarr_asset(self):
        item_no_zarr = MagicMock()
        item_no_zarr.properties = {"proj:code": "EPSG:3413"}
        asset = MagicMock()
        asset.roles = ["data"]
        asset.href = "https://example.com/data.nc"
        item_no_zarr.assets = {"data": asset}

        with _mock_stac_open([item_no_zarr]):
            geom = geometry.mapping(geometry.Point(-45.0, 75.0))
            results = _search_cubes(geom, geom)

        assert len(results) == 0

    def test_returns_empty_on_exception(self):
        with patch(_PATCH_OPEN) as mock_open:
            mock_open.side_effect = Exception("Connection failed")
            geom = geometry.mapping(geometry.Point(-45.0, 75.0))
            results = _search_cubes(geom, geom)
        assert results == []

    def test_missing_proj_code_defaults_to_3413(self):
        mock_item = MagicMock()
        mock_item.properties = {}
        asset = MagicMock()
        asset.roles = ["data"]
        asset.href = (
            "https://its-live-data.s3.amazonaws.com/datacubes/v2/N70W040/"
            "ITS_LIVE_vel_EPSG3413_G0120_X-50000_Y-1650000.zarr"
        )
        mock_item.assets = {"data": asset}

        with _mock_stac_open([mock_item]):
            geom = geometry.mapping(geometry.Point(-45.0, 75.0))
            results = _search_cubes(geom, geom)
        assert results[0]["properties"]["epsg"] == "3413"

    def test_find_by_point_uses_search_cubes(self):
        with patch("itslive.velocity_cubes._cubes._search_cubes") as mock_search:
            from itslive.velocity_cubes import find_by_point

            find_by_point(lon=-45.0, lat=75.0)
            mock_search.assert_called_once()
            args = mock_search.call_args[0]
            assert args[0]["type"] == "Point"

    def test_find_by_bbox_uses_search_cubes(self):
        with patch("itslive.velocity_cubes._cubes._search_cubes") as mock_search:
            from itslive.velocity_cubes import find_by_bbox

            find_by_bbox(-50.0, 65.0, -40.0, 75.0)
            mock_search.assert_called_once()
            args = mock_search.call_args[0]
            assert args[0]["type"] == "Polygon"

    def test_find_by_polygon_uses_search_cubes(self):
        with patch("itslive.velocity_cubes._cubes._search_cubes") as mock_search:
            from itslive.velocity_cubes import find_by_polygon

            points = [
                (-50.0, 65.0), (-40.0, 65.0),
                (-40.0, 75.0), (-50.0, 75.0),
                (-50.0, 65.0),
            ]
            find_by_polygon(points)
            mock_search.assert_called_once()
            args = mock_search.call_args[0]
            assert args[0]["type"] == "Polygon"


class TestSearchCubesIntegration:
    """Integration tests that hit the live STAC API at stac.itslive.cloud."""

    @pytest.mark.integration
    def test_find_by_point_returns_cubes(self):
        from itslive.velocity_cubes import find_by_point

        results = find_by_point(lon=-45.1, lat=75.0)
        assert len(results) >= 1
        for cube in results:
            assert cube["properties"]["zarr_url"].endswith(".zarr")
            assert "epsg" in cube["properties"]

    @pytest.mark.integration
    def test_find_by_bbox_returns_cubes(self):
        from itslive.velocity_cubes import find_by_bbox

        results = find_by_bbox(-50.0, 65.0, -40.0, 75.0)
        assert len(results) >= 1
        for cube in results:
            assert cube["properties"]["zarr_url"].endswith(".zarr")

    @pytest.mark.integration
    def test_public_constants_match_stac_api(self):
        import pystac_client

        client = pystac_client.Client.open(STAC_CATALOG_URL)
        collection = client.get_collection(STAC_COLLECTION)
        assert collection is not None
        assert collection.id == STAC_COLLECTION
