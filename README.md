# ITSLIVE Vortex
<img src="docs/vortex.png" align="middle" width="200px"/>

# A Python client for ITSLIVE glacier velocity data.

## Installing *itslive*

```bash
pip install itslive
```

In addition to NetCDF image pairs and mosaics, ITS_LIVE produces cloud-optimized Zarr data cubes, which contain all image-pair data co-aligned on a common grid for simplified data access. Cloud optimization enable rapid analysis without intermediary APIs or services and ITS_LIVE cubes can map directly into Python xarray or Julia ZArray structures.


This library can be used as a stand alone tool to extract velocity time series from any given lon, lat pair on land glaciers. e.g.

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

 > Operations for the aggegation can be mean,max,min,average,median etc and the frequency is represented with a single character i.e. d=day, w=week, m=month.


Try it in your browser without installing anything! [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/betolink/itslive-vortex/main)

