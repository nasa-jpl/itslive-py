import datetime

import pytest
import rich_click as click

from itslive.cli.export import validate_csv, validate_latitude, validate_longitude
from itslive.cli.search import validate_bbox, validate_date, validate_filter, validate_polygon


class FakeContext:
    pass


class TestValidateBbox:
    def test_valid_bbox(self):
        result = validate_bbox(FakeContext(), None, "-50,65,-40,75")
        assert result == [-50.0, 65.0, -40.0, 75.0]

    def test_invalid_too_few_parts(self):
        with pytest.raises(click.BadParameter):
            validate_bbox(FakeContext(), None, "-50,65")

    def test_invalid_too_many_parts(self):
        with pytest.raises(click.BadParameter):
            validate_bbox(FakeContext(), None, "1,2,3,4,5")

    def test_invalid_lat_out_of_range(self):
        with pytest.raises(click.BadParameter):
            validate_bbox(FakeContext(), None, "-50,-91,-40,75")

    def test_invalid_lon_out_of_range(self):
        with pytest.raises(click.BadParameter):
            validate_bbox(FakeContext(), None, "-181,65,-40,75")

    def test_non_numeric(self):
        with pytest.raises(click.BadParameter):
            validate_bbox(FakeContext(), None, "a,b,c,d")

    def test_none_returns_none(self):
        assert validate_bbox(FakeContext(), None, None) is None


class TestValidatePolygon:
    def test_valid_polygon(self):
        result = validate_polygon(
            FakeContext(), None, "-50,65,-48,65,-48,67,-50,67,-50,65"
        )
        assert result == [-50.0, 65.0, -48.0, 65.0, -48.0, 67.0, -50.0, 67.0, -50.0, 65.0]

    def test_too_few_values(self):
        with pytest.raises(click.BadParameter):
            validate_polygon(FakeContext(), None, "1,2")

    def test_odd_number_of_values(self):
        with pytest.raises(click.BadParameter):
            validate_polygon(FakeContext(), None, "1,2,3")

    def test_none_returns_none(self):
        assert validate_polygon(FakeContext(), None, None) is None


class TestValidateDate:
    def test_valid_date(self):
        result = validate_date(FakeContext(), None, "2020-01-01")
        assert result == datetime.date(2020, 1, 1)

    def test_invalid_format(self):
        with pytest.raises(click.BadParameter):
            validate_date(FakeContext(), None, "01-01-2020")

    def test_none_returns_none(self):
        assert validate_date(FakeContext(), None, None) is None


class TestValidateFilter:
    def test_none_returns_none(self):
        assert validate_filter(FakeContext(), None, None) is None

    def test_empty_tuple(self):
        assert validate_filter(FakeContext(), None, ()) is None

    def test_equals_filter(self):
        result = validate_filter(FakeContext(), None, ("platform:=:S2",))
        assert len(result) == 1
        prop, pf = result[0]
        assert prop == "platform"
        assert pf.op == "="
        assert pf.value == "S2"

    def test_gte_filter_numeric(self):
        result = validate_filter(FakeContext(), None, ("percent_valid_pixels:>=:85",))
        prop, pf = result[0]
        assert prop == "percent_valid_pixels"
        assert pf.op == ">="
        assert pf.value == 85

    def test_gte_filter_float(self):
        result = validate_filter(FakeContext(), None, ("percent_valid_pixels:>=:85.5",))
        prop, pf = result[0]
        assert pf.value == 85.5

    def test_lt_filter(self):
        result = validate_filter(FakeContext(), None, ("count:<:100",))
        prop, pf = result[0]
        assert pf.op == "<"
        assert pf.value == 100

    def test_neq_filter(self):
        result = validate_filter(FakeContext(), None, ("version:!=:002",))
        prop, pf = result[0]
        assert pf.op == "!="
        assert pf.value == "002"

    def test_colon_in_property_name(self):
        result = validate_filter(FakeContext(), None, ("proj:code:=:EPSG:3413",))
        prop, pf = result[0]
        assert prop == "proj:code"
        assert pf.value == "EPSG:3413"

    def test_multiple_filters(self):
        result = validate_filter(
            FakeContext(),
            None,
            ("platform:=:S2", "version:!=:002"),
        )
        assert len(result) == 2

    def test_invalid_operator(self):
        with pytest.raises(click.BadParameter):
            validate_filter(FakeContext(), None, ("platform:^:S2",))

    def test_no_operator(self):
        with pytest.raises(click.BadParameter):
            validate_filter(FakeContext(), None, ("platform:S2",))


class TestValidateLatitude:
    def test_valid_lat(self):
        assert validate_latitude(FakeContext(), None, 70.1) == 70.1

    def test_too_high(self):
        with pytest.raises(click.BadParameter):
            validate_latitude(FakeContext(), None, 91.0)

    def test_too_low(self):
        with pytest.raises(click.BadParameter):
            validate_latitude(FakeContext(), None, -91.0)

    def test_none_returns_none(self):
        assert validate_latitude(FakeContext(), None, None) is None


class TestValidateLongitude:
    def test_valid_lon(self):
        assert validate_longitude(FakeContext(), None, -45.1) == -45.1

    def test_too_high(self):
        with pytest.raises(click.BadParameter):
            validate_longitude(FakeContext(), None, 181.0)

    def test_too_low(self):
        with pytest.raises(click.BadParameter):
            validate_longitude(FakeContext(), None, -181.0)

    def test_none_returns_none(self):
        assert validate_longitude(FakeContext(), None, None) is None


class TestValidateCsv:
    def test_none_returns_none(self):
        assert validate_csv(FakeContext(), None, None) is None
