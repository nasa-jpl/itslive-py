import itslive
import pandas as pd
import rich_click as click
from rich import print as rprint

# Use Rich markup
click.rich_click.USE_RICH_MARKUP = True


class Mutex(click.Option):
    # Taken from https://bit.ly/3hWeXVj wonder if this is already in the library
    def __init__(self, *args, **kwargs):
        self.not_required_if: list = kwargs.pop("not_required_if")

        assert self.not_required_if, "'not_required_if' parameter required"
        kwargs["help"] = (
            f"{kwargs.get('help', '')} Option is mutually exclusive with "
            f"{', '.join(self.not_required_if).strip()}."
        )
        super(Mutex, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        current_opt: bool = self.name in opts
        for mutex_opt in self.not_required_if:
            if mutex_opt in opts:
                if current_opt:
                    error_msg = (
                        f"Illegal usage: {self.name}"
                        f" is mutually exclusive with {str(mutex_opt)}"
                    )
                    raise click.UsageError(error_msg)
                else:
                    self.prompt = None
        return super(Mutex, self).handle_parse_result(ctx, opts, args)


def validate_latitude(lctx, param, lat):
    if lat:
        if lat > 90.0 or lat < -90.0:
            error_msg = (
                f"Not a valid longitude value: {lat}, must be between -90 and 90"
            )
            raise click.BadParameter(error_msg)
        return lat


def validate_longitude(ctx, param, lon):
    if lon:
        if lon > 180.0 or lon < -180.0:
            error_msg = (
                f"Not a valid longitude value: {lon}, must be between -180 and 180"
            )
            raise click.BadParameter(error_msg)
        return lon


def validate_csv(ctx, param, value):
    if value:
        try:
            df = pd.read_csv(value, usecols=[0, 1], names=["lon", "lat"])
            assert df.lat.dtype == "float"
            assert df.lon.dtype == "float"
            assert df.lon.between(-180.0, 180.0).any()
            assert df.lat.between(-90.0, 90.0).any()
            return df
        except Exception:
            raise click.BadParameter("Not a valid CSV file, the format is lon,lat")


def export_time_series(points, variables, format, outdir):
    if format == "csv":
        itslive.velocity_cubes.export_csv(points, variables, outdir)
    elif format == "netcdf":
        itslive.velocity_cubes.export_netcdf(points, variables, outdir)
    else:
        itslive.velocity_cubes.export_stdout(points, variables)
    return None


def plot_time_series(points, variable, label_by, outdir, stdout):
    if stdout is not None:
        # experimental
        itslive.velocity_cubes._plot_time_series_terminal(points, variable)
    else:
        pass
        # TODO: save plot on outdir
        # plot = itslive.velocity_cubes.plot_time_series(points, variable, label_by)
        # plot.save()
    return None


@click.command()
@click.option(
    "--itslive-catalog",
    required=False,
    default="https://its-live-data.s3.amazonaws.com/datacubes/catalog_v02.json",
    help="GeoJSON catalog with the ITS_LIVE cube metadata",
)
@click.option(
    "--input-coordinates",
    required=False,
    cls=Mutex,
    not_required_if=["lat", "lon"],
    type=click.Path(),
    callback=validate_csv,
    help="[magenta bold]Input csv file[/]. [dim] \[format: comma separated lon,lat coordinates][/]",
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
        "v: velocity"
        "v_error: error in v"
        "vx: Velocity x component"
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
    type=click.Choice(["csv", "netcdf", "stdout"]),
    help="export to fortmat",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Verbose output",
)
def export(
    itslive_catalog, input_coordinates, lat, lon, variables, outdir, format, debug
):
    """
    ITS_LIVE Global Glacier Veolocity

    [i]You can try using --help at the top level and also for
    specific group subcommands.[/]
    """

    points = []
    itslive.velocity_cubes.load_catalog(itslive_catalog)
    if debug:
        rprint("Debug mode is [red]on[/]")
        rprint(f"Using: {itslive.velocity_cubes._current_catalog_url}")
    if input_coordinates is not None:
        # rprint(f"input file head: {input.head}")
        for index, row in input_coordinates.iterrows():
            points.append((row["lon"], row["lat"]))
    else:
        if lat and lon:
            points.append((lon, lat))

    if len(points) and format is not None:
        export_time_series(points, variables, format, outdir)
    else:
        rprint(" At least one set of coordinates are needed, --help")
