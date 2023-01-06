from typing import Any

import matplotlib
import numpy as np
import pandas as pd
import plotext as plt
import xarray as xr


def plot_terminal(
    lon: float,
    lat: float,
    dataset: xr.Dataset,
    variable: str = "v",
    operation: str = "median",
    freq: str = "m",
) -> None:
    """Plots a variable from an already located xarray dataset in the terminal"""
    plt.date_form("Y-m-d")
    sat = np.unique(dataset["satellite_img1"].values)
    sats = set([str(s[0]) for s in sat])
    g_ts = dataset.to_pandas().sort_index()

    for satellite in sats:
        ts = getattr(
            (
                g_ts.where(g_ts["satellite_img1"].str[:1] == satellite)[
                    variable
                ].groupby(pd.Grouper(freq=freq))
            ),
            operation,
        )()

        dates = [
            d.to_pydatetime().strftime("%Y-%m-%d")
            for d in pd.date_range(start=ts.index.min(), end=ts.index.max(), freq=freq)
        ]

        values = [float(v[0]) for v in ts.values]
        if satellite in ["1", "2"]:
            label = f"Sentinel {satellite}"
        else:
            label = f"Landsat {satellite}"
        plt.plot(dates, values, label=label)

        xticks, xlabels = list(dates[::6]), list(dates[::6])
        plt.xticks(xticks, xlabels)
    plt.xlabel("Date")
    # TODO: add lat, lon in plot
    plt.ylabel(variable)
    plt.show()


def _plot_by_location(
    lon: float,
    lat: float,
    ax: matplotlib.axes.Axes,
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
    ax: matplotlib.axes.Axes,
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
    ax: matplotlib.axes.Axes,
    dataset: xr.Dataset,
    variable: str = "v",
    label_by: str = "location",
) -> Any:
    """ """
    if label_by == "location":
        return _plot_by_location(lon, lat, ax, dataset, variable)
    else:
        return _plot_by_sensor(lon, lat, ax, dataset, variable)
