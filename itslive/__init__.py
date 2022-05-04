from importlib.metadata import metadata

from .data_cube import Cube
from .data_pairs import Pairs
from .data_viz import Plot

__all__ = ["Pairs", "Cube", "Plot"]

__metadata__ = metadata("itslive")
__version__ = __metadata__["version"]
