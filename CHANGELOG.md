# Changelog

## [Unreleased]


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



