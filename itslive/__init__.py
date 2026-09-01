from importlib.metadata import version

import itslive.velocity_cubes as velocity_cubes
import itslive.velocity_pairs as velocity_pairs
from itslive._search import (
    EQ,
    GT,
    GTE,
    LT,
    LTE,
    NEQ,
    PropertyFilter,
    search,
)

__all__ = [
    "velocity_cubes",
    "velocity_pairs",
    "search",
    "PropertyFilter",
    "EQ",
    "GTE",
    "LTE",
    "GT",
    "LT",
    "NEQ",
]

# this comes from the installed version not the editable source
__version__ = version("itslive")
