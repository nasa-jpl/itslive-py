from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from itslive import search
from itslive._search import (
    EQ,
    WAREHOUSE_HREF,
    _build_search_prefixes,
    _resolve_serverless_opts,
    _ts_bound,
    build_roi,
    build_search_filters,
    serverless_search,
)


def _make_mock_item(href: str):
    """Build a mock STAC item with a single data asset."""
    item = MagicMock()
    asset = MagicMock()
    asset.roles = ["data"]
    asset.href = href
    item.assets = {"data": asset}
    return item


class _FakeDF(dict):
    """Mimics the bit of the pandas DataFrame we use."""

    def to_list(self):
        return list(self["data_href"])


class TestDispatchValidation:
    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid type"):
            search(bbox=[-50, 65, -40, 75], type="magic")

    def test_invalid_engine_raises(self):
        with pytest.raises(ValueError, match="Invalid engine"):
            search(bbox=[-50, 65, -40, 75], type="serverless", engine="spark")

    def test_pgstac_rejects_geoparquet_engine(self):
        with pytest.raises(ValueError, match="not valid with type='pgstac'"):
            search(bbox=[-50, 65, -40, 75], type="pgstac", engine="rustac")

    def test_missing_geometry_raises(self):
        with pytest.raises(ValueError, match="needs a bbox"):
            search()

    def test_unknown_kwarg_raises(self):
        with pytest.raises(TypeError, match="Unexpected search arguments"):
            search(bbox=[-50, 65, -40, 75], nonsense=True)

    def test_warehouse_rejects_resolution_2(self):
        with pytest.raises(ValueError, match="warehouse catalog"):
            search(bbox=[-50, 65, -40, 75], resolution=2)

    def test_invalid_partition_type_raises(self):
        with pytest.raises(ValueError, match="partition_type"):
            search(bbox=[-50, 65, -40, 75], partition_type="quadkey")


class TestBuildRoi:
    def test_bbox(self):
        roi = build_roi(bbox=[-50, 65, -40, 75])
        assert roi["type"] == "Polygon"

    def test_geojson_feature(self):
        geometry = {"type": "Point", "coordinates": [-45, 70]}
        roi = build_roi(geojson={"type": "Feature", "geometry": geometry})
        assert roi == geometry

    def test_flat_polygon_list(self):
        roi = build_roi(polygon=[-50, 65, -40, 65, -40, 75, -50, 65])
        assert roi["type"] == "Polygon"

    def test_no_geometry_raises(self):
        with pytest.raises(ValueError):
            build_roi()

    def test_invalid_geojson_raises(self):
        with pytest.raises(ValueError, match="Invalid GeoJSON"):
            build_roi(geojson={"type": "Toaster"})


class TestBuildSearchFilters:
    def test_percent_valid_pixels(self):
        filters, extra = build_search_filters(percent_valid_pixels=85)
        assert filters["percent_valid_pixels"].value == 85

    def test_zero_percent_no_filter(self):
        filters, _ = build_search_filters(percent_valid_pixels=0)
        assert "percent_valid_pixels" not in filters

    def test_mission_to_platforms(self):
        filters, extra = build_search_filters(mission="sentinel2")
        assert "platform" not in filters
        assert {"op": "in", "args": [{"property": "platform"}, ["S2A", "S2B"]]} in extra

    def test_landsat_mission_uses_lc_platforms(self):
        filters, extra = build_search_filters(mission="landsatoli")
        assert "platform" not in filters
        assert {
            "op": "in",
            "args": [{"property": "platform"}, ["LC08", "LC09", "LO08", "LO09"]],
        } in extra

    def test_mission_alias_l8(self):
        filters, extra = build_search_filters(mission="l8")
        assert {
            "op": "in",
            "args": [{"property": "platform"}, ["LC08", "LO08"]],
        } in extra

    def test_min_and_max_interval_compound(self):
        filters, extra = build_search_filters(min_interval=7, max_interval=30)
        assert "date_dt" not in filters
        assert extra[0]["op"] == "and"

    def test_custom_filters_override(self):
        custom = {"platform": None, "version": "EQ-ignored"}
        from itslive._search import EQ, GTE

        filters, _ = build_search_filters(
            percent_valid_pixels=0,
            filters={"platform": EQ("L9"), "junk": None},
        )
        assert filters["platform"].value == "L9"
        assert "junk" not in filters
        assert custom is not None
        assert GTE is not None


class TestResolveServerlessOpts:
    def test_default_is_warehouse_with_year_partitions(self):
        store, opts = _resolve_serverless_opts(None)
        assert store == WAREHOUSE_HREF
        assert opts["use_year_partitions"] is True
        assert opts["partition_type"] == "h3"
        assert opts["resolution"] == 1

    def test_legacy_href_no_year_partitions(self):
        store, opts = _resolve_serverless_opts(
            "s3://its-live-data/test-space/stac/geoparquet/h3r2", resolution=2
        )
        assert "h3r2" in store
        assert opts["use_year_partitions"] is False

    def test_latlon(self):
        _, opts = _resolve_serverless_opts(
            "s3://its-live-data/test-space/stac/geoparquet/latlon",
            partition_type="latlon",
        )
        assert opts["partition_type"] == "latlon"
        assert opts["use_year_partitions"] is False


class TestPrefixBuilding:
    def test_year_pushdown_expands_range(self):
        opts = {
            "partition_type": "h3",
            "resolution": 1,
            "overlap": "bbox_overlap",
            "reduce_spatial_search": False,
            "use_hive_partitions": True,
            "use_year_partitions": True,
        }
        roi = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
        prefixes = _build_search_prefixes(
            WAREHOUSE_HREF, roi, opts, "2020-01-01", "2022-06-30"
        )
        assert len(prefixes) == 3
        assert all("/year=" in p and p.endswith("/**/*.parquet") for p in prefixes)
        assert any("year=2020" in p for p in prefixes)
        assert any("year=2022" in p for p in prefixes)

    def test_no_year_pushdown_for_legacy(self):
        opts = {
            "partition_type": "h3",
            "resolution": 2,
            "overlap": "bbox_overlap",
            "reduce_spatial_search": False,
            "use_hive_partitions": True,
            "use_year_partitions": False,
        }
        roi = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
        prefixes = _build_search_prefixes(
            "s3://bucket/h3r2", roi, opts, "2020-01-01", "2022-06-30"
        )
        assert prefixes == ["s3://bucket/h3r2/grid=h3/level=2/tile=*/**/*.parquet"]

    def test_spatial_reduce_uses_grid_names(self):
        opts = {
            "partition_type": "latlon",
            "resolution": 1,
            "overlap": "bbox_overlap",
            "reduce_spatial_search": True,
            "use_hive_partitions": False,
            "use_year_partitions": False,
        }
        roi = {"type": "Point", "coordinates": [-45.0, 75.0]}
        with patch("itslive._search.path_exists", return_value=True):
            prefixes = _build_search_prefixes(
                "s3://bucket/latlon", roi, opts, "2020-01-01", "2020-12-31"
            )
        assert prefixes
        assert all(
            "N70" in p or "S70" in p or "N80" in p or "S80" in p for p in prefixes
        )


class TestTsBound:
    def test_date_only_expands(self):
        assert _ts_bound("2020-01-01") == "2020-01-01T00:00:00Z"
        assert _ts_bound("2020-12-31", end=True) == "2020-12-31T23:59:59Z"

    def test_full_timestamp_passthrough(self):
        assert _ts_bound("2020-06-01T12:30:00Z") == "2020-06-01T12:30:00Z"


class TestPgstacSearch:
    @patch("pystac_client.Client.open")
    def test_returns_sorted_deduplicated_list(self, mock_open):
        mock_client = MagicMock()
        mock_search = MagicMock()
        mock_search.items.return_value = [
            _make_mock_item("https://s3/b.nc"),
            _make_mock_item("https://s3/a.nc"),
            _make_mock_item("https://s3/a.nc"),
            _make_mock_item("https://s3/skipme.tif"),  # filtered by asset_type
        ]
        mock_search.items.return_value[3].assets = {
            "data": SimpleNamespace(roles=["data"], href="https://s3/skipme.tif")
        }
        mock_client.search.return_value = mock_search
        mock_open.return_value = mock_client

        urls = search(bbox=[-50, 65, -40, 75], type="pgstac")
        assert urls == ["https://s3/a.nc", "https://s3/b.nc"]

    @patch("pystac_client.Client.open")
    def test_stream_returns_generator(self, mock_open):
        mock_client = MagicMock()
        mock_search = MagicMock()
        mock_search.items.return_value = [_make_mock_item("https://s3/a.nc")]
        mock_client.search.return_value = mock_search
        mock_open.return_value = mock_client

        result = search(bbox=[-50, 65, -40, 75], type="pgstac", stream=True)
        assert list(result) == ["https://s3/a.nc"]

    @patch("pystac_client.Client.open")
    def test_search_kwargs(self, mock_open):
        mock_client = MagicMock()
        mock_search = MagicMock()
        mock_search.items.return_value = []
        mock_client.search.return_value = mock_search
        mock_open.return_value = mock_client

        search(
            bbox=[-50, 65, -40, 75],
            start="2020-01-01",
            end="2020-12-31",
            collection="my-collection",
            percent_valid_pixels=0,
            type="pgstac",
        )

        call_kwargs = mock_client.search.call_args[1]
        assert call_kwargs["collections"] == ["my-collection"]
        assert call_kwargs["datetime"] == "2020-01-01T00:00:00Z/2020-12-31T23:59:59Z"
        assert call_kwargs["limit"] == 10000
        assert "intersects" in call_kwargs
        assert "filter" not in call_kwargs

    @patch("pystac_client.Client.open")
    def test_interval_filter_builds_date_dt_cql2(self, mock_open):
        mock_client = MagicMock()
        mock_search = MagicMock()
        mock_search.items.return_value = []
        mock_client.search.return_value = mock_search
        mock_open.return_value = mock_client

        search(
            bbox=[-50, 65, -40, 75],
            min_interval=7,
            max_interval=30,
            percent_valid_pixels=0,
            type="pgstac",
        )

        cql2 = mock_client.search.call_args[1]["filter"]
        assert cql2["op"] == "and"
        assert {"op": ">=", "args": [{"property": "date_dt"}, 7]} in cql2["args"]
        assert {"op": "<=", "args": [{"property": "date_dt"}, 30]} in cql2["args"]
        assert mock_client.search.call_args[1]["filter_lang"] == "cql2-json"


class _FakeDuckdbConnection:
    """Captures executed SQL and returns a fake arrow record-batch stream."""

    executed = []

    def execute(self, query):
        _FakeDuckdbConnection.executed.append(query)

        class _Result:
            def arrow(self):
                class _Batch:
                    def column(self, name):
                        class _Col:
                            @staticmethod
                            def to_pylist():
                                return ["https://s3/x.nc", "https://s3/skip.tif"]

                        return _Col()

                    def __iter__(self):
                        return iter([self])

                return _Batch()

        return _Result()


class TestServerlessDuckdb:
    @patch("duckdb.connect")
    def test_sql_contains_spatial_temporal_and_property_filters(
        self, mock_connect, monkeypatch
    ):
        monkeypatch.setattr("itslive._search.path_exists", lambda _: True)
        con = _FakeDuckdbConnection()
        mock_connect.return_value = con
        _FakeDuckdbConnection.executed = []

        with patch("h3.h3shape_to_cells_experimental") as mock_cells:
            import h3 as _h3

            mock_cells.return_value = [_h3.latlng_to_cell(70.0, -45.0, 1)]
            urls = search(
                bbox=[-50, 65, -40, 75],
                start="2020-01-01",
                end="2021-06-30",
                type="serverless",
                engine="duckdb",
                filters={"proj:code": EQ("EPSG:3413")},
            )

        assert urls == ["https://s3/x.nc"]
        queries = [q for q in _FakeDuckdbConnection.executed if "SELECT" in q]
        assert queries, "expected at least one data query"
        for q in queries:
            assert "ST_Intersects" in q
            assert "start_datetime <=" in q
            assert "end_datetime >=" in q
            assert "T23:59:59Z" in q  # inclusive end bound
            assert '"proj:code" = ' in q or "proj:code" in q
        # warehouse default: year push-down limits prefixes to the date range
        tile_globs = [q for q in queries if "year=" in q]
        assert tile_globs
        years = {q.split("year=")[1][:4] for q in tile_globs}
        assert years == {"2020", "2021"}

    @patch("duckdb.connect")
    def test_legacy_catalog_has_no_year_globs(self, mock_connect, monkeypatch):
        monkeypatch.setattr("itslive._search.path_exists", lambda _: True)
        con = _FakeDuckdbConnection()
        mock_connect.return_value = con
        _FakeDuckdbConnection.executed = []

        with patch("h3.h3shape_to_cells_experimental") as mock_cells:
            mock_cells.return_value = ["8001fffffffffff"]
            search(
                bbox=[-50, 65, -40, 75],
                start="2020-01-01",
                end="2020-12-31",
                type="serverless",
                engine="duckdb",
                base_catalog_href="s3://its-live-data/test-space/stac/geoparquet/h3r2",
                resolution=2,
            )

        queries = [q for q in _FakeDuckdbConnection.executed if "SELECT" in q]
        assert queries
        assert all("year=" not in q for q in queries)
        assert any("h3r2" in q for q in queries)


class TestBackcompat:
    def test_find_returns_list_with_stac_engine(self):
        with patch("pystac_client.Client.open") as mock_open:
            mock_client = MagicMock()
            mock_search = MagicMock()
            mock_search.items.return_value = [_make_mock_item("https://s3/a.nc")]
            mock_client.search.return_value = mock_search
            mock_open.return_value = mock_client

            from itslive.velocity_pairs import find

            urls = find(bbox=[-50, 65, -40, 75], engine="stac")
        assert urls == ["https://s3/a.nc"]

    def test_find_streaming_maps_rustac_to_serverless(self):
        with patch("itslive._search._iter_rustac") as mock_iter:
            mock_iter.return_value = iter(["https://s3/a.nc", "https://s3/b.nc"])
            from itslive.velocity_pairs import find_streaming

            urls = list(
                find_streaming(
                    bbox=[-50, 65, -40, 75],
                    engine="rustac",
                    start="2020-01-01",
                    end="2020-12-31",
                )
            )
        assert urls == ["https://s3/a.nc", "https://s3/b.nc"]
        assert mock_iter.call_args[0][0]  # prefixes were resolved

    def test_find_streaming_rejects_unknown_engine(self):
        from itslive.velocity_pairs import find_streaming

        with pytest.raises(ValueError, match="Invalid engine"):
            find_streaming(bbox=[-50, 65, -40, 75], engine="spark")


class TestServerlessSearchShim:
    def test_deprecated_wrapper_warns_and_forwards(self, monkeypatch):
        monkeypatch.setattr("itslive._search.path_exists", lambda _: True)

        with (
            patch("duckdb.connect") as mock_connect,
            patch("h3.h3shape_to_cells_experimental") as mock_cells,
        ):
            mock_cells.return_value = ["8001fffffffffff"]
            con = _FakeDuckdbConnection()
            mock_connect.return_value = con
            _FakeDuckdbConnection.executed = []

            with pytest.warns(DeprecationWarning, match="deprecated"):
                urls = serverless_search(
                    "2020-01-01",
                    "2020-12-31",
                    {
                        "type": "Polygon",
                        "coordinates": [
                            [[-50, 65], [-40, 65], [-40, 75], [-50, 75], [-50, 65]]
                        ],
                    },
                )

        assert urls == ["https://s3/x.nc"]
        queries = [q for q in _FakeDuckdbConnection.executed if "SELECT" in q]
        assert any("year=2020" in q for q in queries)
