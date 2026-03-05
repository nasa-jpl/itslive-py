[![Citation](https://zenodo.org/badge/486012726.svg)](https://doi.org/10.5281/zenodo.16969191)


# ITS_LIVEpy

# A Python client for ITSLIVE glacier velocity data.

<p align="center">

<a href="https://pypi.org/project/itslive" target="_blank">
    <img src="https://img.shields.io/pypi/v/itslive?color=%2334D058&label=pypi%20package" alt="Package version">
</a>

<a href="https://pypi.org/project/itslive/" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/itslive.svg" alt="Python Versions">
</a>

<a href='https://itslive.readthedocs.io/en/latest/?badge=latest'>
    <img src='https://readthedocs.org/projects/itslive/badge/?version=latest' alt='Documentation Status' />
</a>

</p>

For information on running tests, bumping versions, CI workflows, and building
documentation locally, see the [Development Guide](DEVELOPMENT.md).

## Installing *itslive*

```bash
pip install itslive
```

Or with Conda

```bash
conda install -c conda-forge itslive
```

In addition to NetCDF image pairs and mosaics, ITS_LIVE produces cloud-optimized Zarr data cubes, which contain all image-pair data co-aligned on a common grid for simplified data access. Cloud optimization enables rapid analysis without intermediary APIs or services and ITS_LIVE cubes can map directly into Python xarray or Julia ZArray structures.

This library can be used as a stand alone tool to extract velocity time series from any given lon, lat pair on land glaciers. e.g.


### Using a Jupyter notebook

```python
import itslive

points = [(-47.1, 70.1),
          (-46.1, 71.2)]

velocities = itslive.velocity_cubes.get_time_series(points=points)
```

### Using terminal

```bash
itslive-export --lat 70.153 --lon -46.231 --format csv --outdir greenland
```

`netcdf` and `csv` formats are supported. We can print the output to stdout with:

```bash
itslive-export --lat 70.153 --lon -46.231 --format stdout
```

We can also plot any of the ITS_LIVE variables directly on the terminal by executing `itslive-plot`, e.g.

```bash
itslive-plot --lat 70.1 --lon -46.1 --variable v
```

> Operations for the aggregation can be mean, max, min, average, median etc and the frequency is represented with a single character i.e. d=day, w=week, m=month.

## Searching ITS_LIVE Granules

The library provides a unified search interface for finding ITS_LIVE velocity granules using three different backends: **STAC API** (default), **geoparquet with duckdb**, and **geoparquet with rustac**. The geoparquet engines are optimized for large-scale searches and can efficiently handle millions of results by streaming them to stdout without loading everything into memory.

All geometry inputs — bounding box, polygon coordinate list, or a GeoJSON geometry dict — are accepted by both the Python API and the CLI.

### Example 1: Using STAC API (default)

The STAC API engine is the default and is suitable for most use cases. It queries the ITS_LIVE STAC catalog at `https://stac.itslive.cloud` via HTTP.

**CLI usage:**
```bash
# Search by bounding box and pipe results to a file
itslive-search --bbox -50,65,-40,75 \
  --start 2020-01-01 \
  --end 2022-12-31 \
  > urls.txt

# Search by polygon (lon,lat pairs, comma-separated)
itslive-search --polygon -50,65,-48,65,-48,67,-50,67,-50,65 \
  --start 2020-01-01 \
  --end 2022-12-31

# Filter by mission and image-pair interval
itslive-search --bbox -50,65,-40,75 \
  --mission sentinel1 \
  --min-interval 12 \
  --max-interval 36 \
  --filter percent_valid_pixels:>=:80
```

**Python usage:**
```python
import itslive
from datetime import date

# Search by bounding box
urls = itslive.velocity_pairs.find(
    bbox=[-50, 65, -40, 75],
    start=date(2020, 1, 1),
    end=date(2022, 12, 31),
)

# Search by polygon (list of (lon, lat) tuples)
urls = itslive.velocity_pairs.find(
    polygon=[(-50, 65), (-48, 65), (-48, 67), (-50, 67)],
    start=date(2020, 1, 1),
    end=date(2022, 12, 31),
)

# Search by GeoJSON geometry dict
geojson = {
    "type": "Polygon",
    "coordinates": [[[-79.2, -2.7], [-78.3, -2.7], [-78.3, -1.8], [-79.2, -1.8], [-79.2, -2.7]]]
}
urls = itslive.velocity_pairs.find(
    geojson=geojson,
    start=date(2020, 1, 1),
    end=date(2022, 12, 31),
)
```

### Example 2: Using geoparquet with duckdb

The duckdb engine queries geoparquet files directly from S3, which is faster for large spatial queries and can handle massive result sets efficiently. It uses H3 hexagonal spatial indexing for optimized spatial searches.

**CLI usage:**
```bash
# Search a large region using geoparquet
itslive-search --bbox -50,65,-40,75 \
  --engine duckdb \
  --partition-type h3 \
  --resolution 2 \
  > urls.txt

# Search by polygon
itslive-search --polygon -50,65,-48,65,-48,67,-50,67,-50,65 \
  --engine duckdb \
  --format json > results.json
```

**Python usage:**
```python
import itslive

# Search with bounding box
urls = itslive.velocity_pairs.find(
    bbox=[-50, 65, -40, 75],
    engine="duckdb",
    partition_type="h3",
    resolution=2,
)

# Stream results (recommended for large result sets)
for url in itslive.velocity_pairs.find_streaming(
    bbox=[-50, 65, -40, 75],
    engine="duckdb"
):
    # Process each URL immediately without loading all into memory
    print(url)
```

### Example 3: Using geoparquet with rustac

The rustac engine is similar to duckdb but uses a Rust-based implementation for potentially better performance on complex queries. It also queries geoparquet files with H3 spatial indexing.

**CLI usage:**
```bash
# Search using rustac engine
itslive-search --bbox -50,65,-40,75 \
  --engine rustac \
  --format csv > results.csv

# Combine with property filters
itslive-search --bbox -50,65,-40,75 \
  --engine rustac \
  --filter platform:=:S2 \
  --filter version:!=:002 \
  --count-only
```

**Python usage:**
```python
import itslive
from itslive.search import EQ, NEQ, GTE

# Use rustac with advanced property filters
urls = itslive.velocity_pairs.find(
    bbox=[-50, 65, -40, 75],
    engine="rustac",
    filters={
        "platform": EQ("S2"),
        "version": NEQ("002"),
        "percent_valid_pixels": GTE(85),
        "proj:code": EQ("EPSG:3413")
    }
)
```

### Comparison of Search Engines

| Engine | Best For | Speed | Memory Usage |
|--------|----------|-------|--------------|
| **stac** | General use, small-to-medium queries | Medium | Low |
| **duckdb** | Large areas, millions of results | Fast (direct S3) | Low (streaming) |
| **rustac** | Complex queries, high performance | Very Fast (direct S3) | Low (streaming) |

### CLI Output Formats

The `itslive-search` command supports multiple output formats:

```bash
# URL format (default) - one URL per line
itslive-search --bbox -50,65,-40,75 > urls.txt

# JSON format - array of URLs
itslive-search --bbox -50,65,-40,75 --format json > urls.json

# CSV format - URLs with filename metadata
itslive-search --bbox -50,65,-40,75 --format csv > urls.csv

# Count only - just the number of matching URLs
itslive-search --bbox -50,65,-40,75 --count-only
```

### Filtering Options

You can filter granules by any STAC property using the `--filter` option (CLI) or `filters` parameter (Python).

```bash
# Multiple filters via CLI
itslive-search --bbox -50,65,-40,75 \
  --filter platform:=:S2 \
  --filter version:=:002 \
  --filter percent_valid_pixels:>=:85 \
  --filter proj:code:=:EPSG:3413
```

```python
from itslive.search import EQ, GTE

urls = itslive.velocity_pairs.find(
    bbox=[-50, 65, -40, 75],
    filters={
        "platform": EQ("S2"),
        "percent_valid_pixels": GTE(85),
    }
)
```

Supported operators: `=` (equals), `>=` (greater or equal), `<=` (less or equal), `>` (greater), `<` (less), `!=` (not equal).

### Geoparquet Catalog Paths

When using `duckdb` or `rustac` engines, the catalog path is built automatically from `--partition-type` and `--resolution`. Currently only **H3 resolutions 1 and 2** are available.

```
--partition-type h3 --resolution 1  →  s3://its-live-data/test-space/stac/geoparquet/h3r1
--partition-type h3 --resolution 2  →  s3://its-live-data/test-space/stac/geoparquet/h3r2
--partition-type latlon             →  s3://its-live-data/test-space/stac/geoparquet/latlon
```

Use `--base-catalog-href` to override with an explicit path.

The STAC engine always uses `https://stac.itslive.cloud` — no extra configuration needed.

Try it in your browser without installing anything! [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/betolink/itslive-vortex/main)
