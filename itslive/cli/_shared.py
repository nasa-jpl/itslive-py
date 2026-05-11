import pandas as pd
import rich_click as click


class Mutex(click.Option):
    """Option class for mutually exclusive CLI options."""

    def __init__(self, *args, **kwargs):
        self.not_required_if: list = kwargs.pop("not_required_if")
        assert self.not_required_if, "'not_required_if' parameter required"
        kwargs["help"] = (
            f"{kwargs.get('help', '')} Option is mutually exclusive with "
            f"{', '.join(self.not_required_if).strip()}."
        )
        super().__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        current_opt: bool = self.name in opts
        for mutex_opt in self.not_required_if:
            if mutex_opt in opts:
                if current_opt:
                    raise click.UsageError(
                        f"Illegal usage: {self.name} "
                        f"is mutually exclusive with {str(mutex_opt)}"
                    )
                else:
                    self.prompt = None
        return super().handle_parse_result(ctx, opts, args)


def validate_latitude(ctx, param, lat):
    if lat is not None and not (-90.0 <= lat <= 90.0):
        raise click.BadParameter(
            f"Invalid latitude: {lat}, must be between -90 and 90"
        )
    return lat


def validate_longitude(ctx, param, lon):
    if lon is not None and not (-180.0 <= lon <= 180.0):
        raise click.BadParameter(
            f"Invalid longitude: {lon}, must be between -180 and 180"
        )
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
    return value
