from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotext
import xarray as xr

# Import numpy explicitly if not already imported
try:
    import numpy as np
except ImportError:
    import numpy as np

# import itslive


def plot_terminal(
    lon: float,
    lat: float,
    dataset: xr.Dataset,
    variables: list[str] = ["v"],
    operation: str = "median",
    freq: str = "m",
    color_by_sensor: bool = True,
) -> None:
    """Plots one or more variables from a given lon, lat in the terminal

    Args:
        lon: Longitude coordinate
        lat: Latitude coordinate
        dataset: xarray Dataset containing the time series data
        variables: List of variable names to plot (e.g., ["v", "vx", "vy"])
        operation: Aggregation operation (median, mean, max, min)
        freq: Resampling frequency (d=daily, w=weekly, m=monthly)
        color_by_sensor: If True, color points by satellite/sensor
    """
    # Variable metadata for better labeling
    var_labels = {
        "v": "Velocity (m/yr)",
        "v_error": "Velocity Error (m/yr)",
        "vx": "Vx (m/yr)",
        "vx_error": "Vx Error (m/yr)",
        "vy": "Vy (m/yr)",
        "vy_error": "Vy Error (m/yr)",
        "count": "Count",
        "v0": "Climatological Velocity (m/yr)",
        "dv_dt": "Velocity Change (m/yr²)",
    }

    satellite_labels = {
        "1": "S1",
        "2": "S2",
        "4": "L4",
        "5": "L5",
        "7": "L7",
        "8": "L8",
        "9": "L9",
    }

    plot_colors = [
        "red",
        "blue",
        "green",
        "yellow",
        "magenta",
        "cyan",
    ]

    for var_idx, variable in enumerate(variables):
        if variable not in dataset:
            print(f"Warning: Variable '{variable}' not found in dataset")
            continue

        # Convert to pandas and process
        ts = dataset[variable].to_pandas().sort_index()
        ts = ts[~ts.index.duplicated(keep="first")].resample("ME").max().ffill()

        date_strs = [d.strftime("%Y-%m-%d") for d in ts.index.to_pydatetime()]
        values = [float(v) for v in ts.values]

        # Set title and labels
        title = f"ITS_LIVE: {var_labels.get(variable, variable)}"
        subtitle = f"Location: ({lon:.4f}, {lat:.4f})"

        # Compute year-based xtick positions using plotext's date string API
        if date_strs:
            min_year = ts.index[0].year
            max_year = ts.index[-1].year
            num_years = max_year - min_year + 1
        else:
            min_year = max_year = 2000
            num_years = 1
        # Choose step so at most ~13 ticks are shown, always on even/round years
        step = max(1, (num_years + 12) // 13)
        tick_dates = [f"{y}-01-31" for y in range(min_year, max_year + 1, step)]
        tick_labels = [str(y) for y in range(min_year, max_year + 1, step)]

        plotext.plotsize(120, 30)
        plotext.date_form("Y-m-d")
        plotext.xticks(tick_dates, tick_labels)
        plotext.title(f"{title} | {subtitle}")
        plotext.ylabel(var_labels.get(variable, variable))
        plotext.xlabel("Date")

        # Plot with different colors if multiple variables — use date strings
        # so plotext's native date axis handles tick spacing correctly
        color = plot_colors[var_idx % len(plot_colors)] if len(variables) > 1 else "red"
        plotext.plot(date_strs, values, marker="braille", color=color, label=variable)

        plotext.show()

        # Add statistics after the plot (filter out NaN values)
        valid_values = [v for v in values if not pd.isna(v)]
        if valid_values:
            mean_val = sum(valid_values) / len(valid_values)
            max_val = max(valid_values)
            print(f"\n{var_labels.get(variable, variable)} Statistics:")
            print(f"  Mean: {mean_val:.2f} m/yr")
            print(f"  Max:  {max_val:.2f} m/yr")
            print(f"  N:    {len(valid_values)} data points")

        # Color by satellite if requested and satellite data is available
        if color_by_sensor and "satellite_img1" in dataset:
            print(f"\nSatellite Distribution for {variable}:")
            sats = dataset["satellite_img1"].values
            unique_sats, counts = np.unique(sats, return_counts=True)
            for sat, count in zip(unique_sats, counts):
                sat_label = satellite_labels.get(str(sat), str(sat))
                print(f"  {sat_label}: {count} observations")

        print()  # Add spacing between variables


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
