import pkg_resources  # type: ignore

from . import data_cube as cubes

__all__ = ["cubes"]

__version__ = pkg_resources.get_distribution("itslive").version
