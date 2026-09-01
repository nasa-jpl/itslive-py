# Changelog

## [0.7.0] - 2026-09-01

* features
    * new unified `itslive.search(...)` API with `type="serverless"` (STAC geoparquet via duckdb/rustac) and `type="pgstac"` (STAC API via pystac-client); `stream=True` yields hrefs for 1M+ result sets; filter helpers (`EQ`, `GTE`, `LTE`, `GT`, `LT`, `NEQ`, `PropertyFilter`) exported at the top level (`from itslive import EQ, search`)
    * new default serverless catalog `s3://its-live-data/test-space/stac/catalog/warehouse` (overridable via `base_catalog_href`); date range is pushed down to `year=` Hive partitions on the warehouse layout for faster temporal pruning
    * duckdb reads stream results as Arrow record batches (constant memory, no pandas materialization) and set `preserve_insertion_order=false`
    * rustac upgraded to the 0.9.x API (items parsed from JSON strings, fixed broken engine); optional `arro3-core` (`[arrow]` extra) enables the `search_to_arrow` fast path with a JSON fallback
    * CLI: new `--type {serverless,pgstac}` flag (inferred from `--engine` when omitted); `--use-hive-partitions` now defaults to on
* bug fixes
    * duckdb now `INSTALL`/`LOAD httpfs` explicitly (previously relied on auto-install, which could hang on S3 reads)
    * pgstac searches sent no `limit`, forcing the API's default 10-item pages (thousands of round-trips for large queries); now defaults to `limit=10000`; bare `datetime` values are normalized to full UTC timestamps (some pgstac deployments reject them)
    * CLI `--count-only` printed nothing with the default `url` format; empty-result CSV counting referenced an unbound variable
    * `find_streaming` now validates the engine eagerly so bad arguments raise at call time instead of on first iteration
* breaking changes
    * the `itslive.search` module is renamed to `itslive._search`; `from itslive.search import EQ` must become `from itslive import EQ` (filter helpers and `search()` are exported from the top level)
    * `serverless_search` is deprecated in favor of `search(type="serverless", ...)`
    * the default geoparquet catalog changed from the legacy `h3r1`/`h3r2` paths to the warehouse, which only provides H3 resolution 1 — pass `base_catalog_href` explicitly to query legacy stores
* maintenance
    * `find`/`find_streaming` are now thin wrappers over `search()`, removing ~200 lines of duplicated STAC/geoparquet logic
    * added `pyrightconfig.json` and fixed 22 pre-existing type errors across the package and tests
    * raised the `rustac` floor to `>=0.9` and added the optional `arrow` extra (`arro3-core`)
    * Python floor raised to `>=3.11` (required by rustac 0.9.x); CI matrix updated accordingly
    * moved `jupyterlab` out of core dependencies into the optional `notebooks` extra

## [0.6.1] - 2026-05-11

* bug fixes
    * fixed STAC catalog URL in CLI help and docstrings (`stac.its-live.org` → `stac.itslive.cloud`)
    * fixed H3 resolution labels: resolution 1 is coarser, 2 is finer (were swapped)
    * fixed CLI docstring example using invalid `--resolution 3` (only 1, 2 valid)
    * fixed `export.py` help string collapsed by formatter (implicit string concatenation)
* documentation
    * clarified geoparquet comment in README and docs index
    * removed mkdocstrings plugin (unused, was slowing build from minutes to 0.3s)
    * removed mkdocs-jupyter and jupyter-related docs dependencies (conflict with mkdocs-material)
    * loosened pinned dependency versions (earthaccess, pandas, pyproj, Shapely, xarray, zarr, etc.)
* maintenance
    * excluded `notebooks/` from ruff lint (legacy code, not part of the library)
    * added `--exclude notebooks` to CI ruff check command
    * applied ruff formatting across all source, test, and notebook files
    * removed `widgetsnbextension`, `ipympl`, `ipywidgets`, `jupyterlab`, `dask`, `h5netcdf` from docs extras


* [v0.6.0] 2026-05-10
* features
    * parquet export format (`itslive-export --format parquet`)
    * quickstart documentation and mkdocs GitHub Pages workflow
    * exported STAC constants (`STAC_CATALOG_URL`, `STAC_COLLECTION`)
* bug fixes
    * fixed `get_time_series` returning no data for Antarctic coordinates (issue #9)
    * fixed `duckdb`/`rustac` unconditional imports causing `ModuleNotFoundError`
    * fixed `min_interval`/`max_interval` using wrong STAC property (`date_dt`, not `min_interval_days`)
    * fixed `_download_nsidc` typo (`auhtenticated` → `authenticated`)
    * fixed CLI filter parsing dropping zero-padded strings (`"002"` → `2`)
* maintenance
    * added `duckdb`, `rustac`, `h3`, `pyarrow` to core dependencies
    * removed orphaned/duplicate code (`plot_time_series` stub, unused constants)
    * modernized type annotations (`List[str]` → `list[str]`, `Optional` → `|`)
    * fixed all 9 pre-existing test failures (h3 v4, Python 3.13 compat, etc.)
    * comprehensive unit tests for cube STAC search, parquet export, interval filters

* [v0.5.1] 2026-03-05
* features
    * updated catalog to Feb 2026
    * added support for geoparquet-based queries (bulk queries)
    * improved performance of data retrieval and processing
    * enhanced visualization capabilities with new plotting options

* [v0.3.2] 2024-01-22
* features
    * fixed catalog URL 
    * loosen strict dependencies for cryocloud
    * Added scene pair velocity search and download API
    * Terminal plot can be consolidated or grouped by satellite

## [v0.1.7] 2023-05-25
* features:
    * Mark added a function to find the neares cube for edge cases
    * reorganized imports to avoid circular dependencies
    * new example notebooks

## [v0.1.6] 2022-12-29
* features
    * implemented plotting methods

## [v0.1.5] 2022-12-01

* features
    * implemented plot in stdout with plotext

## [v0.1.4] 2022-11-28

* bug fixes
    * can handle empty output parameters in cli.

## [v0.1.3] 2022-11-28

* features
    * added cli so it can be used standalone.

## [v0.1.2] 2022-11-17

* initial release:



