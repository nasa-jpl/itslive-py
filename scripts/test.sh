#!/usr/bin/env bash

set -e
set -x

pytest -m "not integration" --cov=itslive --cov-report=term-missing ${@}
bash ./scripts/lint.sh
