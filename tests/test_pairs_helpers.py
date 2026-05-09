import datetime

from itslive.velocity_pairs._pairs import coverage


class TestCoverage:
    def test_returns_empty_list(self):
        result = coverage(bbox=[-50, 65, -40, 75])
        assert result == []


class TestFindStreamingGeometry:
    """Indirect tests via _pairs internal logic."""

    def test_module_imports(self):
        from itslive.velocity_pairs import find, find_streaming

        assert find is not None
        assert find_streaming is not None
