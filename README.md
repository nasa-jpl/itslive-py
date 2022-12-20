# ITSLIVE Vortex
<img src="docs/vortex.png" align="middle" width="200px"/>

# A Python client for ITSLIVE glacier velocity data.

## Installing *itslive*

```bash
pip install itslive
```

In addition to NetCDF image pairs and mosaics, ITS_LIVE produces cloud-optimized Zaar datacubes, which contain all image-pair data, co-aligned on a common grid for simplified data access. Cloud optimization enables rapid analysis without intermediary data servers, and ITS_LIVE datacubes map directly into Python xarray or Julia ZArray structures. ITS_LIVE provides basic access and plotting tools in both Python and Julia, making it easy to incorporate the datacubes into workflows locally or on remote servers.


This library can be used as a stand alone command to extract velocity time series from any given lon, lat pair on land glaciers. e.g.

```bash
itslive-export --lat 70.153 --lon -46.231 --format csv --outdir greenland
```

`netcdf` and `csv` formats are supported. We can print the output to stdout with:

```bash
itslive-export --lat 70.153 --lon -46.231 --format stdout
```

Lastly we can plot any of the available variables in our terminal by executing `itslive-plot`, e.g.

```bash
itslive-plot --lat 70.1 --lon -46.1 --variable vx
```


Try it in your browser without installing anything! [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/betolink/itslive-vortex/main)

