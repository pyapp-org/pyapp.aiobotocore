"""
pyApp - AIO Botocore Extension

"""
from .__version__ import __version__
from .factory import *

version_info = tuple(int(v) for v in __version__.split("."))


class Extension:
    """
    pyApp AIOBotocore Extension
    """

    default_settings = ".default_settings"
    checks = ".checks"
