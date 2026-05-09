from unittest.mock import patch

from itslive.search import bucket_cube_name_from_url, path_exists


class TestPathExists:
    @patch("os.path.exists", return_value=True)
    def test_local_path_exists(self, mock_exists):
        assert path_exists("/some/local/path") is True
        mock_exists.assert_called_once_with("/some/local/path")

    @patch("os.path.exists", return_value=False)
    def test_local_path_not_exists(self, mock_exists):
        assert path_exists("/some/local/path") is False

    @patch("s3fs.S3FileSystem")
    def test_s3_path_exists(self, mock_s3fs):
        fs_instance = mock_s3fs.return_value
        fs_instance.exists.return_value = True
        assert path_exists("s3://bucket/key") is True
        mock_s3fs.assert_called_once_with(anon=True)
        fs_instance.exists.assert_called_once_with("s3://bucket/key")

    @patch("s3fs.S3FileSystem")
    def test_s3_path_not_exists(self, mock_s3fs):
        fs_instance = mock_s3fs.return_value
        fs_instance.exists.return_value = False
        assert path_exists("s3://bucket/key") is False
