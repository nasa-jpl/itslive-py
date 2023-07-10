import logging

import pytest
from itslive import velocity_pairs as pairs

logger = logging.getLogger(__name__)

base_url = "https://nsidc.org/apps/itslive-search/velocities/urls/"


def test_imports():
    functions = ["find", "coverage", "download"]
    from itslive import velocity_pairs

    assert velocity_pairs

    for f in functions:
        assert hasattr(velocity_pairs, f)
        assert callable(getattr(velocity_pairs, f))


def test_load_fails_with_empty_parameters():
    with pytest.raises(Exception):
        scenes = pairs.find(bbox=[], version=2)
        assert type(scenes) is list
        assert len(scenes) > 0
