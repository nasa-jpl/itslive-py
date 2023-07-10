from importlib.metadata import version

import itslive.velocity_cubes as velocity_cubes
import itslive.velocity_pairs as velocity_pairs

__all__ = ["velocity_cubes", "velocity_pairs"]

# this comes from the installed version not the editable source
__version__ = version("itslive")
