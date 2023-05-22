from importlib.metadata import version

import itslive.velocity_cubes._cubes as velocity_cube

__all__ = ["velocity_cube", "velocity_pairs"]

# this comes from the installed version not the editable source
__version__ = version("itslive")
