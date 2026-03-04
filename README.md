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

In addition to NetCDF image pairs and mosaics, ITS_LIVE produces cloud-optimized Zarr data cubes, which contain all image-pair data co-aligned on a common grid for simplified data access. Cloud optimization enable rapid analysis without intermediary APIs or services and ITS_LIVE cubes can map directly into Python xarray or Julia ZArray structures.


This library can be used as a stand alone tool to extract velocity time series from any given lon, lat pair on land glaciers. e.g.


### Using a Jupyter notebook

```python
import itslive

points=[(-47.1, 70.1),
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

> Note: For cubes, the `use_stac` parameter is `True` by default, meaning they are searched using the STAC API "itslive-cubes" collection. Set `use_stac=False` to use the legacy GeoJSON catalog (not recommended).

We can also plot any of the ITS_LIVE variables directly on the terminal by executing `itslive-plot`, e.g.

```bash
itslive-plot --lat 70.1 --lon -46.1 --variable v
```

 > Operations for the aggegation can be mean,max,min,average,median etc and the frequency is represented with a single character i.e. d=day, w=week, m=month.

## Searching ITS_LIVE Granules

The library provides a unified search interface for finding ITS_LIVE velocity granules using three different backends: **STAC API** (default), **geoparquet with duckdb**, and **geoparquet with rustac**. The geoparquet engines are optimized for large-scale searches and can efficiently handle millions of results by streaming them to stdout without loading everything into memory.

### Example 1: Using STAC API (default)

The STAC API engine is the default and is suitable for most use cases. It queries the ITS_LIVE STAC catalog via HTTP.

**CLI usage:**
```bash
# Search and pipe results to file
itslive-search --bbox -50,65,-40,75 \
  --start 2020-01-01 \
  --end 2022-12-31 \
  > urls.txt

# Filter by mission and date range
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

# Basic search
urls = itslive.velocity_pairs.find(
    bbox=[-50, 65, -40, 75],
    start=date(2020, 1, 1),
    end=date(2022, 12, 31),
    engine="stac"  # Default, can be omitted
)

# With filters
from itslive.search import EQ, GTE

urls = itslive.velocity_pairs.find(
    bbox=[-50, 65, -40, 75],
    mission="sentinel1",
    percent_valid_pixels=80,
    engine="stac"
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

# Use custom geoparquet parameters for finer control
itslive-search --bbox -50,65,-40,75 \
  --engine duckdb \
  --partition-type h3 \
  --resolution 3 \
  --overlap bbox_overlap \
  --reduce-spatial-search \
  --format json > results.json
```

**Python usage:**
```python
import itslive

# Search with custom geoparquet parameters
urls = itslive.velocity_pairs.find(
    bbox=[-50, 65, -40, 75],
    engine="duckdb",
    partition_type="h3",
    resolution=2,
    overlap="bbox_overlap",
    reduce_spatial_search=True
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

The rustac engine is similar to duckdb but uses a Rust-based implementation for potentially better performance, especially for complex queries. It also queries geoparquet files with H3 spatial indexing.

**CLI usage:**
```bash
# Search using rustac engine
itslive-search --bbox -50,65,-40,75 \
  --engine rustac \
  --format csv > results.csv

# Combine with filters for precise queries
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

# Use rustac with advanced filters
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

# Stream millions of URLs efficiently
count = 0
for url in itslive.velocity_pairs.find_streaming(
    bbox=[-50, 65, -40, 75],
    engine="rustac",
    filters={"mission": EQ("landsatOLI")}
):
    count += 1
    # Process URL
    with open(f"urls_{count}.txt", "a") as f:
        f.write(url + "\n")
```

### Comparison of Search Engines

| Engine | Best For | Speed | Memory Usage | Use Case |
|---------|-----------|--------|---------------|-----------|
| **stac** | General use, small queries | Medium | Low | Daily use, simple queries |
| **duckdb** | Large areas, millions of results | Fast (geoparquet) | Low (streaming) | Regional analysis, bulk downloads |
| **rustac** | Complex queries, high performance | Very Fast (geoparquet) | Low (streaming) | Performance-critical applications |

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

You can filter granules by any STAC property using the `--filter` option (CLI) or `filters` parameter (Python):

```bash
# Multiple filters
itslive-search --bbox -50,65,-40,75 \
  --filter platform:=:S2 \
  --filter version:=:002 \
  --filter percent_valid_pixels:>=:85
```

Supported operators: `=` (equals), `>=` (greater or equal), `<=` (less or equal), `>` (greater), `<` (less), `!=` (not equal).

### Geoparquet Catalog Paths

When using `duckdb` or `rustac` engines, the geoparquet catalog path is constructed based on the spatial partitioning scheme and resolution.

**Important:** Currently, only **H3 resolutions r1 and r2** are available for granule searches.

**H3 Hexagonal Partitioning:**
```
--partition-type h3 --resolution 2 → s3://its-live-data/test-space/stac/geoparquet/h3r2  (primary)
--partition-type h3 --resolution 1 → s3://its-live-data/test-space/stac/geoparquet/h3r1
```

**Lat/Lon Geographic Partitioning:**
```
--partition-type latlon → s3://its-live-data/test-space/stac/geoparquet/latlon
```

**Custom Paths:**
Use `--base-catalog-href` to specify an explicit catalog path, which overrides the automatic path construction above.

**STAC API:**
```
--engine stac → https://stac.its-live.org (default, no --base-catalog-href needed)
```

### Engine Comparison

| Engine | Catalog Type | Best For | Speed | Memory Usage |
|---------|---------------|-----------|--------|---------------|
| **stac** | STAC API (itslive-granules/itslive-cubes) | General use, small queries | Medium | Low |
| **duckdb** | Geoparquet (H3 r1/r2 only) | Large areas, millions of results | Fast (direct S3) | Low (streaming) |
| **rustac** | Geoparquet (H3 r1/r2 only) | Complex queries, high performance | Very Fast (direct S3) | Low (streaming) |
--partition-type h3 --resolution 2  →  s3://its-live-data/test-space/stac/geoparquet/h3r2
--partition-type h3 --resolution 3  →  s3://its-live-data/test-space/stac/geoparquet/h3r3
--partition-type h3 --resolution 4  →  s3://its-live-data/test-space/stac/geoparquet/h3r4
--partition-type h3 --resolution 5  →  s3://its-live-data/test-space/stac/geoparquet/h3r5
```

**Lat/Lon Geographic Partitioning:**
```
--partition-type latlon  →  s3://its-live-data/test-space/stac/geoparquet/latlon
```

**Custom Paths:**
Use `--base-catalog-href` to specify an explicit catalog path, which overrides the automatic path construction above.

**STAC API:**
```
--engine stac  →  https://stac.its-live.org (default, no --base-catalog-href needed)
```

Higher H3 resolution values (3-5) provide finer spatial indexing for more precise queries, while lower values (0-2) provide coarser indexing but may query fewer files.


Try it in your browser without installing anything! [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/betolink/itslive-vortex/main)

