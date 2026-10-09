"""A coordinate of exactly 0 is a real place, not a missing argument."""

import pytest
from click.testing import CliRunner

from itslive.cli.export import export
from itslive.cli.plot import plot

MISSING = "At least one set of coordinates are needed"


@pytest.fixture
def runner():
    return CliRunner()


@pytest.mark.parametrize("lon,lat", [("0", "78"), ("10", "0"), ("0", "0")])
def test_export_accepts_zero_coordinate(runner, lon, lat):
    result = runner.invoke(
        export, ["--lon", lon, "--lat", lat, "--format", "stdout"], catch_exceptions=True
    )

    assert MISSING not in result.output


@pytest.mark.parametrize("lon,lat", [("0", "78"), ("10", "0"), ("0", "0")])
def test_plot_accepts_zero_coordinate(runner, lon, lat):
    result = runner.invoke(plot, ["--lon", lon, "--lat", lat], catch_exceptions=True)

    assert MISSING not in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["--format", "stdout"],
        ["--lat", "78", "--format", "stdout"],
        ["--lon", "0", "--format", "stdout"],
    ],
)
def test_export_still_reports_absent_coordinates(runner, args):
    result = runner.invoke(export, args, catch_exceptions=True)

    assert MISSING in result.output
