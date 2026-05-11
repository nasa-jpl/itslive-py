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


def export_time_series(points, variables, format, outdir):
    if format == "csv":
        itslive.velocity_cubes.export_csv(points, variables, outdir)
    elif format == "netcdf":
        itslive.velocity_cubes.export_netcdf(points, variables, outdir)
    elif format == "parquet":
        itslive.velocity_cubes.export_parquet(points, variables, outdir)
    else:
        itslive.velocity_cubes.export_stdout(points, variables)
    return None


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
    "--variables",
    type=click.Choice(["v", "v_error", "vy", "vx"], case_sensitive=False),
    multiple=True,
    default=["v"],
    help=(
        "v: velocity, "
        "v_error: error in v, "
        "vx: Velocity x component, "
        "vy: Velocity y component"
    ),
)
@click.option(
    "--outdir",
    type=str,
    help="output directory",
)
@click.option(
    "--format",
    type=click.Choice(["csv", "netcdf", "parquet", "stdout"]),
    help="export to fortmat",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Verbose output",
)
def export(input_coordinates, lat, lon, variables, outdir, format, debug):
    """
    ITS_LIVE Global Glacier Veolocity

    [i]You can try using --help at the top level and also for
    specific group subcommands.[/]
    """

    points = []
    if debug:
        rprint("Debug mode is [red]on[/]")
        rprint("Using STAC catalog: https://stac.itslive.cloud/")
    if input_coordinates is not None:
        for index, row in input_coordinates.iterrows():
            points.append((row["lon"], row["lat"]))
    else:
        if lat and lon:
            points.append((lon, lat))

    if len(points) and format is not None:
        export_time_series(points, variables, format, outdir)
    else:
        rprint(" At least one set of coordinates are needed, --help")
