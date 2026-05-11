import rich_click as click
from rich import print as rprint

import itslive
from itslive.cli._shared import (
    Mutex,
    validate_csv,
    validate_latitude,
    validate_longitude,
)

# Use Rich markup
click.rich_click.USE_RICH_MARKUP = True


def plot_time_series(points, variable, operation, freq, outdir, stdout):
    itslive.velocity_cubes.plot_time_series_terminal(points, variable)
    return None


def list_variables():
    itslive.velocity_cubes.list_variables()


@click.command()
@click.option(
    "--input-coordinates",
    required=False,
    cls=Mutex,
    not_required_if=["lat", "lon"],
    type=click.Path(),
    callback=validate_csv,
    help="[magenta bold]Input csv file[/]. [dim] [format: comma separated lon,lat coordinates][/]",
)
@click.option(
    "--lat",
    cls=Mutex,
    not_required_if=["input_coordinates"],
    type=float,
    callback=validate_latitude,
    help="Latitude, e.g. 70.1",
)
@click.option(
    "--lon",
    cls=Mutex,
    not_required_if=["input_coordinates"],
    type=float,
    callback=validate_longitude,
    help="Longitude, e.g. -120.4",
)
@click.option(
    "--variable",
    type=click.Choice(["v", "v_error", "vy", "vx"], case_sensitive=False),
    multiple=True,
    default=["v"],
    help=(
        "v: velocity"
        "v_error: error in v"
        "vx: Velocity x component"
        "vy: Velocity y component"
        "for a full list visit itslive"
    ),
)
@click.option(
    "--agg",
    type=str,
    multiple=False,
    default="median-m",
    required=False,
    help=(
        "Aggregation for a given frequency separated by a hyphen, aggregation-frequency"
        "examples: monthly mean woould be: mean-m, weekly median: median-w"
    ),
)
@click.option(
    "--outdir",
    cls=Mutex,
    not_required_if=["stdout"],
    help="output directory",
)
@click.option(
    "--stdout",
    cls=Mutex,
    not_required_if=["outdir"],
    is_flag=True,
    help="Verbose output",
)
def plot(input_coordinates, lat, lon, variable, agg, outdir, stdout):
    """
    ITS_LIVE Global Glacier Veolocity

    [i]You can try using --help at the top level and also for
    specific group subcommands.[/]
    """

    points = []
    rprint("Using STAC catalog: https://stac.itslive.cloud/")
    if input_coordinates is not None:
        # rprint(f"input file head: {input.head}")
        for index, row in input_coordinates.iterrows():
            points.append((row["lon"], row["lat"]))
    else:
        if lat and lon:
            points.append((lon, lat))

    if len(points):
        aggregation = agg.split("-")
        operation = aggregation[0]
        freq = aggregation[1]
        plot_time_series(points, list(variable), operation, freq, outdir, stdout)
    else:
        rprint(
            "At least one set of coordinates are needed, here is a list of the available variables: \n"
        )
        list_variables()
