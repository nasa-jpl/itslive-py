from datetime import date
from typing import List

import pandas as pd
import plotext as plt
import xarray as xr

import itslive


def plot_terminal(
    lon: float, lat: float, dataset: xr.Dataset, variable: str = "v"
) -> None:
    """Plots a variable from a given lon, lat in the terminal"""
    plt.date_form("Y-m-d")
    # the beauty of pandas + xarray
    ts = dataset[variable].to_pandas().sort_index()
    dates = [d for d in ts.index.to_pydatetime()]
    values = [float(v[0]) for v in ts.values]
    xticks = [i for i in range(0, len(dates))]
    xlabels = [d.strftime("%Y/%m") for d in dates]
    plt.xticks(xticks, xlabels)
    plt.plot(values)
    plt.show()


def _plot_by_location(points: List[tuple[float, float]], variable: str = "v") -> None:
    return None


def _plot_by_sensor(points: List[tuple[float, float]], variable: str = "v") -> None:
    return None


def plot_variable(
    points: List[tuple[float, float]], variable: str = "v", label_by: str = "location"
) -> None:
    return None
