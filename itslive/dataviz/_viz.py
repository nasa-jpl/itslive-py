from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotext
import xarray as xr

# import itslive


def plot_terminal(
    lon: float,
    lat: float,
    dataset: xr.Dataset,
    variable: str = "v",
    operation: str = "median",
    freq: str = "m",
) -> None:
    """Plots a variable from a given lon, lat in the terminal"""
    plotext.date_form("Y-m-d")
    # the beauty of pandas + xarray
    ts = dataset[variable].to_pandas().sort_index()
    dates = [d for d in ts.index.to_pydatetime()]
    values = [float(v[0]) for v in ts.values]
    xticks = [i for i in range(0, len(dates))]
    xlabels = [d.strftime("%Y/%m") for d in dates]
    plotext.xticks(xticks, xlabels)
    plotext.plot(values)
    plotext.show()


def _plot_by_location(
    lon: float,
    lat: float,
    ax: plt.Axes,
    dataset: xr.Dataset,
    variable: str = "v",
) -> None:
    dt = dataset["date_dt"].values
    dt = dt.astype(float) * 1.15741e-14

    ax.plot(
        dataset["mid_date"],
        dataset[variable],
        label=f"Lat: {lat},  Lon: {lon}",
    )

    return ax


def _plot_by_sensor(
    lon: float,
    lat: float,
    ax: plt.Axes,
    dataset: xr.Dataset,
    variable: str = "v",
) -> None:
    sats = np.unique(dataset["satellite_img1"].values)
    sat_plotsym_dict = {
        "1": "r+",
        "2": "bo",
        "4": "y+",
        "5": "y+",
        "7": "c+",
        "8": "g*",
        "9": "m^",
    }

    sat_label_dict = {
        "1": "Sentinel 1",
        "2": "Sentinel 2",
        "4": "Landsat 4",
        "5": "Landsat 5",
        "7": "Landsat 7",
        "8": "Landsat 8",
        "9": "Landsat 9",
    }

    ax.set_xlabel("Date")
    ax.set_ylabel("Speed (m/yr)")
    ax.set_title("ITS_LIVE Ice Flow Speed m/yr")

    dt = dataset["date_dt"].values
    dt = dt.astype(float) * 1.15741e-14

    for satellite in sats:
        ax.plot(
            dataset["mid_date"].where((dataset["satellite_img1"] == satellite)),
            dataset[variable].where((dataset["satellite_img1"] == satellite)),
            sat_plotsym_dict[satellite],
            markersize=3,
            label=sat_label_dict[satellite],
        )

    return ax


def plot_variable(
    lon: float,
    lat: float,
    ax: plt.Axes,
    dataset: xr.Dataset,
    variable: str = "v",
    label_by: str = "location",
) -> Any:
    """ """
    if label_by == "location":
        return _plot_by_location(lon, lat, ax, dataset, variable)
    else:
        return _plot_by_sensor(lon, lat, ax, dataset, variable)
