from importlib.metadata import version

from itslive import data_cube

__all__ = ["data_cube"]

# this comes from the installed version not the editable source
__version__ = version("itslive")
