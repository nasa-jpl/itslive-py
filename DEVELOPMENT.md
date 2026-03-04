# Development Guide

This document covers everything needed to contribute to **itslive-py**: setting up
a local environment, running tests, checking code style, bumping versions, and
understanding how CI and documentation builds work.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Local Setup](#local-setup)
- [Running Tests](#running-tests)
- [Code Style](#code-style)
- [Versioning with bump-my-version](#versioning-with-bump-my-version)
- [CI Workflows](#ci-workflows)
- [ReadTheDocs](#readthedocs)

---

## Prerequisites

- Python 3.10 or newer
- [Poetry](https://python-poetry.org/docs/#installation) _(recommended)_ or pip
- [pipx](https://pipx.pypa.io/) for installing CLI tools globally

```bash
pip install pipx
pipx install bump-my-version
```

---

## Local Setup

### With Poetry (recommended)

```bash
git clone https://github.com/nasa-jpl/itslive-py.git
cd itslive-py
pip install poetry
poetry install
```

All development tools (`bump-my-version`, `black`, `isort`, `pytest`, etc.) and
docs dependencies are installed automatically.

### With pip

```bash
git clone https://github.com/nasa-jpl/itslive-py.git
cd itslive-py

# Core package + dev tools (linting, testing, versioning)
pip install -e ".[dev]"

# Also install docs dependencies
pip install -e ".[dev,docs]"
```

The `dev` extra includes: `bump-my-version`, `black`, `isort`, `pylint`,
`pytest`, `responses`, and type stubs.  
The `docs` extra adds: `mkdocs`, `mkdocs-material`, `mkdocstrings`, and notebook
dependencies.

---

## Running Tests

Tests are split into two groups using pytest markers:

| Marker | Description | Requires network? |
|---|---|---|
| _(none / default)_ | Fast offline unit tests | No |
| `integration` | Live calls to S3 / STAC endpoints | Yes |

### Unit tests only (used in CI)

```bash
poetry run pytest -m "not integration"
```

### All tests, including integration

```bash
poetry run pytest
```

### Run a specific test file or marker

```bash
# single file
poetry run pytest tests/test_data_cubes.py

# only integration tests
poetry run pytest -m integration
```

Pytest configuration lives in `pyproject.toml` under `[tool.pytest.ini_options]`.
The default `addopts` adds `-v --tb=short` so you get verbose output without
needing to type extra flags.

### Writing new tests

- Place offline, fast tests in the relevant `tests/test_*.py` file with **no
  marker** (or `@pytest.mark.unit`).
- Decorate any test that calls a live endpoint with `@pytest.mark.integration`.
- Use the `mock_responses` fixture from `tests/conftest.py` to stub HTTP calls
  without hitting the network:

```python
import responses as responses_lib

def test_catalog_parse(mock_responses):
    mock_responses.add(
        responses_lib.GET,
        "https://stac.itslive.cloud/",
        json={"type": "Catalog"},
        status=200,
    )
    # call code that makes HTTP requests — no real network traffic occurs
    ...
```

---

## Code Style

The project uses **black** for formatting and **isort** for import ordering.

```bash
# Check only (same as CI)
poetry run black itslive tests --check
poetry run isort --multi-line=3 --trailing-comma --force-grid-wrap=0 \
    --combine-as --line-width 88 --check-only --thirdparty itslive \
    itslive tests

# Auto-fix
poetry run black itslive tests
poetry run isort --multi-line=3 --trailing-comma --force-grid-wrap=0 \
    --combine-as --line-width 88 --thirdparty itslive \
    itslive tests
```

Or run the existing helper script:

```bash
bash scripts/lint.sh
```

---

## Versioning with bump-my-version

The project uses [bump-my-version](https://callowayproject.github.io/bump-my-version/)
to automate version bumps. Configuration lives in `pyproject.toml` under
`[tool.bumpversion]`.

The **only file that needs to change** is `pyproject.toml` — the version exposed
by the package at runtime is read from the installed package metadata via
`importlib.metadata`.

### Bump commands

```bash
# Patch release: 0.3.2 → 0.3.3
bump-my-version bump patch

# Minor release: 0.3.2 → 0.4.0
bump-my-version bump minor

# Major release: 0.3.2 → 1.0.0
bump-my-version bump major
```

Each command:

1. Updates `version` in `pyproject.toml`.
2. Creates a git commit with message `chore: bump version from X to Y`.
3. Creates a git tag `vX.Y.Z`.

Push the commit **and** the tag to trigger the publish workflow:

```bash
git push && git push --tags
```

### Preview without committing

```bash
bump-my-version bump patch --dry-run --verbose
```

---

## CI Workflows

Two GitHub Actions workflows live in `.github/workflows/`.

### `ci.yml` — runs on every push and pull request

Triggers on pushes and PRs targeting `main` or `develop`.

| Job | Python versions | What it does |
|---|---|---|
| `lint` | 3.12 | black + isort checks |
| `test` | 3.10, 3.11, 3.12 | `pytest -m "not integration"` |

Concurrent runs on the same branch/PR are cancelled automatically to save
runner minutes.

### `publish.yml` — runs on version tags

Triggers when a `v*.*.*` tag is pushed (i.e. after `bump-my-version` + `git push --tags`).

1. Builds the distribution with `poetry build`.
2. Publishes to PyPI using [PyPA's official action](https://github.com/pypa/gh-action-pypi-publish)
   with **OIDC trusted publishing** — no `PYPI_TOKEN` secret is required as long
   as the `itslive` project on PyPI is configured with a trusted publisher for
   this repository.

---

## ReadTheDocs

Documentation is built with [MkDocs](https://www.mkdocs.org/) using the
Material theme. Configuration is in `mkdocs.yml`.

ReadTheDocs build configuration is in `.readthedocs.yaml` at the root of the
repository. It:

- Uses Python 3.12 on Ubuntu 22.04.
- Installs dependencies via `poetry install`.
- Builds with MkDocs (not Sphinx).

### Build docs locally

```bash
poetry run mkdocs serve
```

Then open `http://127.0.0.1:8000` in your browser. Changes to files listed
under `watch:` in `mkdocs.yml` trigger a live reload.

### Build a static site

```bash
poetry run mkdocs build
```

Output goes to `site/` (git-ignored).
