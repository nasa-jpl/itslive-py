#!/usr/bin/env bash

set -e
set -x

ruff check itslive tests
black itslive tests --check
isort --profile black --check-only itslive tests
