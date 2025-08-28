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
### Using the terminal

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

