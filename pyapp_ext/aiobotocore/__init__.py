"""
pyApp - AIOBotocore extension

"""
from .factory import aio_create_client
from .factory import create_client
from .factory import get_session
from .factory import session_factory
from .factory import Session


class Extension:
    """
    pyApp AIOBotocore Extension
    """

    default_settings = ".default_settings"
    checks = ".checks"
