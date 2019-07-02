from pyapp.checks.registry import register

from .factory import session_factory, client_factory

register(session_factory)
register(client_factory)
