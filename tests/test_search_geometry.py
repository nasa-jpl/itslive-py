from itslive.search import bucket_cube_name_from_url, point_to_prefix, transform_coord


class TestBucketCubeNameFromUrl:
    def test_extracts_bucket_and_path(self):
        url = "s3://its-live-data/datacubes/v2/N70W040/file.zarr"
        bucket, path = bucket_cube_name_from_url(url)
        assert bucket == "its-live-data"
        assert path == "datacubes/v2/N70W040/file.zarr"

    def test_handles_nested_path(self):
        url = "s3://bucket/a/b/c/d"
        bucket, path = bucket_cube_name_from_url(url)
        assert bucket == "bucket"
        assert path == "a/b/c/d"


class TestPointToPrefix:
    def test_northern_hemisphere_positive_lat_lon(self):
        result = point_to_prefix(75.0, -45.0)
        assert result == "N70W040"

    def test_southern_hemisphere(self):
        result = point_to_prefix(-76.1, -10.0)
        assert result == "S70W010"

    def test_northern_hemisphere_positive_lon(self):
        result = point_to_prefix(33.5, 76.2)
        assert result == "N30E070"

    def test_lat_at_zero(self):
        result = point_to_prefix(0.0, 0.0)
        assert result == "N00E000"

    def test_lat_at_90_clamps(self):
        result = point_to_prefix(90.0, 0.0)
        assert result == "N80E000"

    def test_lon_at_180_clamps(self):
        result = point_to_prefix(0.0, 180.0)
        assert result == "N00E170"

    def test_with_dir_path(self):
        result = point_to_prefix(75.0, -45.0, dir_path="/some/path")
        assert result == "/some/path/N70W040"

    def test_negative_lon(self):
        result = point_to_prefix(0.0, -100.0)
        assert result == "N00W100"


class TestTransformCoord:
    def test_transform_4326_to_3413(self):
        """Use a longitude off the central meridian so both x and y are non-zero."""
        x, y = transform_coord("4326", "3413", -50.0, 75.0)
        assert isinstance(x, float)
        assert isinstance(y, float)
        assert abs(x) > 0
        assert abs(y) > 0

    def test_transform_on_central_meridian(self):
        """EPSG:3413 central meridian is -45°, so lon=-45 gives x=0."""
        x, y = transform_coord("4326", "3413", -45.0, 75.0)
        assert x == 0.0
        assert abs(y) > 0

    def test_transform_roundtrip(self):
        lon, lat = -50.0, 75.0
        x, y = transform_coord("4326", "3413", lon, lat)
        lon2, lat2 = transform_coord("3413", "4326", x, y)
        assert abs(lon - lon2) < 0.1
        assert abs(lat - lat2) < 0.1
