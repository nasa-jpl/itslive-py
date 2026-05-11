from unittest.mock import MagicMock, patch

from itslive.velocity_pairs._pairs import find_streaming


def _make_mock_item(href: str):
    """Build a mock STAC item with a single data asset."""
    item = MagicMock()
    asset = MagicMock()
    asset.roles = ["data"]
    asset.href = href
    item.assets = {"data": asset}
    return item


class TestIntervalFilter:
    """Verify min_interval / max_interval build correct date_dt CQL2 filters."""

    @patch("pystac_client.Client.open")
    def test_min_and_max_interval_use_date_dt(self, mock_open):
        mock_client = MagicMock()
        mock_search = MagicMock()
        mock_search.items.return_value = [_make_mock_item("https://s3/data.nc")]
        mock_client.search.return_value = mock_search
        mock_open.return_value = mock_client

        list(
            find_streaming(
                bbox=[-50, 65, -40, 75],
                min_interval=7,
                max_interval=30,
                start="2014-01-01",
                end="2014-06-01",
                percent_valid_pixels=0,
                engine="stac",
            )
        )

        call_kwargs = mock_client.search.call_args[1]
        cql2 = call_kwargs.get("filter")
        assert cql2 is not None, "Expected a CQL2 filter"
        assert cql2["op"] == "and"
        args = cql2["args"]
        date_dt_ge = {"op": ">=", "args": [{"property": "date_dt"}, 7]}
        date_dt_le = {"op": "<=", "args": [{"property": "date_dt"}, 30]}
        assert date_dt_ge in args, f"Expected date_dt>=7 in {args}"
        assert date_dt_le in args, f"Expected date_dt<=30 in {args}"

    @patch("pystac_client.Client.open")
    def test_min_interval_only(self, mock_open):
        mock_client = MagicMock()
        mock_search = MagicMock()
        mock_search.items.return_value = []
        mock_client.search.return_value = mock_search
        mock_open.return_value = mock_client

        list(
            find_streaming(
                bbox=[-50, 65, -40, 75],
                min_interval=12,
                start="2014-01-01",
                end="2014-06-01",
                percent_valid_pixels=0,
                engine="stac",
            )
        )

        call_kwargs = mock_client.search.call_args[1]
        cql2 = call_kwargs.get("filter")
        assert cql2 is not None
        assert cql2["op"] == ">="
        assert cql2["args"][0] == {"property": "date_dt"}
        assert cql2["args"][1] == 12

    @patch("pystac_client.Client.open")
    def test_max_interval_only(self, mock_open):
        mock_client = MagicMock()
        mock_search = MagicMock()
        mock_search.items.return_value = []
        mock_client.search.return_value = mock_search
        mock_open.return_value = mock_client

        list(
            find_streaming(
                bbox=[-50, 65, -40, 75],
                max_interval=36,
                start="2014-01-01",
                end="2014-06-01",
                percent_valid_pixels=0,
                engine="stac",
            )
        )

        call_kwargs = mock_client.search.call_args[1]
        cql2 = call_kwargs.get("filter")
        assert cql2 is not None
        assert cql2["op"] == "<="
        assert cql2["args"][0] == {"property": "date_dt"}
        assert cql2["args"][1] == 36

    @patch("pystac_client.Client.open")
    def test_no_interval_no_date_dt_filter(self, mock_open):
        mock_client = MagicMock()
        mock_search = MagicMock()
        mock_search.items.return_value = []
        mock_client.search.return_value = mock_search
        mock_open.return_value = mock_client

        list(
            find_streaming(
                bbox=[-50, 65, -40, 75],
                start="2014-01-01",
                end="2014-06-01",
                percent_valid_pixels=0,
                engine="stac",
            )
        )

        call_kwargs = mock_client.search.call_args[1]
        cql2 = call_kwargs.get("filter")
        assert cql2 is None
