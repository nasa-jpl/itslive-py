# Quickstart

## Installation

```bash
pip install itslive
```

## Extract velocity time series at a point

Get annual ice velocity at a single coordinate in Greenland:

```python
import itslive

points = [(-47.1, 70.1)]
ts = itslive.velocity_cubes.get_time_series(points, variables=["v"])

# ts is a list of dicts with xarray Datasets
velocities = ts[0]["time_series"]
print(velocities)
```

## Export to CSV, NetCDF, or Parquet

### CLI

```bash
# CSV export
itslive-export --lat 70.153 --lon -46.231 --format csv --outdir greenland

# NetCDF export
itslive-export --lat 70.153 --lon -46.231 --format netcdf --outdir greenland

# Parquet export
itslive-export --lat 70.153 --lon -46.231 --format parquet --outdir greenland

# Print to terminal
itslive-export --lat 70.153 --lon -46.231 --format stdout
```

### Python API

```python
import itslive

itslive.velocity_cubes.export_csv(
    [(-47.1, 70.1), (-46.1, 71.2)],
    variables=["v", "v_error"],
    outdir="greenland",
)

itslive.velocity_cubes.export_parquet(
    [(-47.1, 70.1)],
    variables=["v"],
    outdir="greenland",
)
```

## Search for velocity pairs (granules)

```python
from datetime import date
import itslive

urls = itslive.velocity_pairs.find(
    bbox=[-50, 65, -40, 75],
    start=date(2020, 1, 1),
    end=date(2022, 12, 31),
    min_interval=7,
    max_interval=30,
    percent_valid_pixels=10,
)

for url in urls[:3]:
    print(url)
```

### CLI equivalent

```bash
itslive-search --bbox -50,65,-40,75 \
  --start 2020-01-01 \
  --end 2022-12-31 \
  --min-interval 7 \
  --max-interval 30
```

## Plot in the terminal

```bash
itslive-plot --lat 70.1 --lon -46.1 --variable v
```

## Available variables

```python
import itslive
itslive.velocity_cubes.list_variables()
```

| Variable | Description |
|----------|-------------|
| `v` | Ice velocity magnitude [m/yr] |
| `v_error` | Ice velocity magnitude error [m/yr] |
| `vx` | Ice velocity x-component [m/yr] |
| `vx_error` | Ice velocity x-component error [m/yr] |
| `vy` | Ice velocity y-component [m/yr] |
| `vy_error` | Ice velocity y-component error [m/yr] |
| `date_dt` | Time separation between image pair [days] |
| `satellite_img1` | Satellite name for image 1 |
| `mission_img1` | Mission name for image 1 |

## Getting help

```bash
itslive-export --help
itslive-search --help
itslive-plot --help
```
