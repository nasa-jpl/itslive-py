from unittest.mock import patch

import pytest

from itslive.search import get_overlapping_grid_names


class TestGetOverlappingGridNamesLatlon:
    @patch("itslive.search.path_exists", return_value=True)
    def test_small_bbox_in_northern_hemisphere(self, mock_path_exists):
        geojson = {
            "type": "Polygon",
            "coordinates": [[[-50, 65], [-40, 65], [-40, 75], [-50, 75], [-50, 65]]],
        }
        result = get_overlapping_grid_names(
            geojson_geometry=geojson,
            base_href="s3://bucket/stac/geoparquet/latlon",
            partition_type="latlon",
        )
        assert len(result) > 0
        for path in result:
            assert path.startswith("s3://bucket/stac/geoparquet/latlon/")
            assert path.endswith("/**/*.parquet")

    @patch("itslive.search.path_exists", return_value=True)
    def test_returns_paths_for_all_missions(self, mock_path_exists):
        geojson = {
            "type": "Point",
            "coordinates": [-45.0, 75.0],
        }
        result = get_overlapping_grid_names(
            geojson_geometry=geojson,
            base_href="s3://bucket/stac/geoparquet/latlon",
            partition_type="latlon",
        )
        missions_in_results = set()
        for path in result:
            parts = path.split("/")
            mission = parts[-4]
            missions_in_results.add(mission)
        assert missions_in_results == {"landsatOLI", "sentinel1", "sentinel2"}

    @patch("itslive.search.path_exists", return_value=False)
    def test_empty_when_no_paths_exist(self, mock_path_exists):
        geojson = {
            "type": "Point",
            "coordinates": [-45.0, 75.0],
        }
        result = get_overlapping_grid_names(
            geojson_geometry=geojson,
            base_href="s3://bucket/stac/geoparquet/latlon",
            partition_type="latlon",
        )
        assert result == []


class TestGetOverlappingGridNamesH3:
    def _small_polygon(self):
        return {
            "type": "Polygon",
            "coordinates": [[[-50, 65], [-40, 65], [-40, 75], [-50, 75], [-50, 65]]],
        }

    @patch("itslive.search.path_exists", return_value=True)
    def test_h3_partitioning_returns_prefixes(self, mock_path_exists):
        result = get_overlapping_grid_names(
            geojson_geometry=self._small_polygon(),
            base_href="s3://bucket/stac/geoparquet/h3",
            partition_type="h3",
            resolution=1,
        )
        assert len(result) > 0
        for path in result:
            assert path.startswith("s3://bucket/stac/geoparquet/h3/")
            assert path.endswith("/**/*.parquet")

    @patch("itslive.search.path_exists", return_value=True)
    def test_h3_with_hive_partitions(self, mock_path_exists):
        result = get_overlapping_grid_names(
            geojson_geometry=self._small_polygon(),
            base_href="s3://bucket/stac/geoparquet/h3",
            partition_type="h3",
            resolution=1,
            use_hive_partitions=True,
        )
        assert len(result) > 0
        for path in result:
            assert "grid=h3" in path
            assert "level=1" in path

    def test_raises_on_unknown_partition(self):
        with pytest.raises(NotImplementedError):
            get_overlapping_grid_names(
                geojson_geometry=self._small_polygon(),
                partition_type="unknown",
            )
