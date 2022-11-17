import pkg_resources  # type: ignore

from itslive import data_cube as cubes

__all__ = ["cubes"]

# this comes from the installed version not the editable source
__version__ = pkg_resources.get_distribution("itslive").version
