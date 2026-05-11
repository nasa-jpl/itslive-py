import pytest

from itslive.search import (
    EQ,
    GT,
    GTE,
    LT,
    LTE,
    NEQ,
    PropertyFilter,
    build_cql2_filter,
    build_cql2_filters_from_dict,
    build_default_filters,
    expr_to_sql,
    filters_to_where,
)


class TestPropertyFilter:
    def test_namedtuple_creation(self):
        pf = PropertyFilter("=", "EPSG:3413")
        assert pf.op == "="
        assert pf.value == "EPSG:3413"

    def test_is_namedtuple(self):
        pf = PropertyFilter(">=", 85.0)
        assert isinstance(pf, tuple)
        assert type(pf).__name__ == "PropertyFilter"


class TestFilterHelpers:
    def test_eq(self):
        pf = EQ("S2")
        assert pf.op == "="
        assert pf.value == "S2"

    def test_gte(self):
        pf = GTE(85.0)
        assert pf.op == ">="
        assert pf.value == 85.0

    def test_lte(self):
        pf = LTE(100.0)
        assert pf.op == "<="
        assert pf.value == 100.0

    def test_gt(self):
        pf = GT(50)
        assert pf.op == ">"
        assert pf.value == 50

    def test_lt(self):
        pf = LT(10)
        assert pf.op == "<"
        assert pf.value == 10

    def test_neq(self):
        pf = NEQ("002")
        assert pf.op == "!="
        assert pf.value == "002"


class TestBuildCql2FiltersFromDict:
    def test_empty_dict(self):
        assert build_cql2_filters_from_dict({}) == []

    def test_single_filter(self):
        filters = {"platform": EQ("S2")}
        result = build_cql2_filters_from_dict(filters)
        assert result == [{"op": "=", "args": [{"property": "platform"}, "S2"]}]

    def test_multiple_filters(self):
        filters = {"platform": EQ("S2"), "percent_valid_pixels": GTE(85.0)}
        result = build_cql2_filters_from_dict(filters)
        assert len(result) == 2
        assert {"op": "=", "args": [{"property": "platform"}, "S2"]} in result
        assert {
            "op": ">=",
            "args": [{"property": "percent_valid_pixels"}, 85.0],
        } in result

    def test_numeric_value(self):
        filters = {"percent_valid_pixels": GTE(80)}
        result = build_cql2_filters_from_dict(filters)
        assert result[0]["args"][1] == 80

    def test_raises_on_non_propertyfilter(self):
        with pytest.raises(TypeError, match="must be a PropertyFilter"):
            build_cql2_filters_from_dict({"platform": "S2"})


class TestBuildDefaultFilters:
    def test_default_values(self):
        filters = build_default_filters("3413")
        assert "percent_valid_pixels" in filters
        assert "proj:code" in filters
        assert filters["proj:code"] == EQ("EPSG:3413")
        assert filters["percent_valid_pixels"] == GTE(1.0)

    def test_custom_percent(self):
        filters = build_default_filters("3031", percent_valid_pixels=85.0)
        assert filters["percent_valid_pixels"] == GTE(85.0)


class TestExprToSql:
    def test_equals(self):
        expr = {"op": "=", "args": [{"property": "platform"}, "S2"]}
        assert expr_to_sql(expr) == "platform = 'S2'"

    def test_gte(self):
        expr = {"op": ">=", "args": [{"property": "percent_valid_pixels"}, 85.0]}
        assert expr_to_sql(expr) == "percent_valid_pixels >= 85.0"

    def test_lte_number(self):
        expr = {"op": "<=", "args": [{"property": "max_interval_days"}, 36]}
        assert expr_to_sql(expr) == "max_interval_days <= 36"

    def test_neq(self):
        expr = {"op": "!=", "args": [{"property": "version"}, "002"]}
        assert expr_to_sql(expr) == "version <> '002'"

    def test_neq_operator_variant(self):
        expr = {"op": "<>", "args": [{"property": "version"}, "002"]}
        assert expr_to_sql(expr) == "version <> '002'"

    def test_property_with_colon_is_quoted(self):
        expr = {"op": "=", "args": [{"property": "proj:code"}, "EPSG:3413"]}
        assert expr_to_sql(expr) == '"proj:code" = \'EPSG:3413\''

    def test_value_with_colon_not_quoted_as_property(self):
        expr = {"op": "=", "args": [{"property": "proj:code"}, "EPSG:3413"]}
        result = expr_to_sql(expr)
        assert "proj:code" in result


class TestFiltersToWhere:
    def test_single_filter(self):
        filters = [{"op": "=", "args": [{"property": "platform"}, "S2"]}]
        assert filters_to_where(filters) == "platform = 'S2'"

    def test_multiple_filters_anded(self):
        filters = [
            {"op": ">=", "args": [{"property": "percent_valid_pixels"}, 85.0]},
            {"op": "=", "args": [{"property": "platform"}, "S2"]},
        ]
        result = filters_to_where(filters)
        assert "AND" in result
        assert "percent_valid_pixels >= 85.0" in result
        assert "platform = 'S2'" in result

    def test_empty_list(self):
        assert filters_to_where([]) == ""


class TestBuildCql2Filter:
    def test_none_for_empty_list(self):
        assert build_cql2_filter([]) is None

    def test_single_expr_returned_as_is(self):
        filters = [{"op": "=", "args": [{"property": "platform"}, "S2"]}]
        assert build_cql2_filter(filters) == filters[0]

    def test_multiple_wrapped_in_and(self):
        filters = [
            {"op": "=", "args": [{"property": "platform"}, "S2"]},
            {"op": ">=", "args": [{"property": "percent_valid_pixels"}, 85.0]},
        ]
        result = build_cql2_filter(filters)
        assert result == {"op": "and", "args": filters}
