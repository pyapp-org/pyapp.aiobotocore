import aiobotocore

from botocore.session import Session
from pyapp.conf.helpers import ThreadLocalNamedSingletonFactory

__all__ = ("Session", "session_factory", "get_session", "create_client")


class SessionFactory(ThreadLocalNamedSingletonFactory[Session]):
    """
    Factory for creating AWS sessions.
    """

    defaults = {
        "aws_access_key_id": None,
        "aws_secret_access_key": None,
        "aws_session_token": None,
    }
    optional_keys = ["region", "endpoint_url", "profile"]

    def create(self, name: str = None) -> Session:
        config = self.get(name)
        session = aiobotocore.get_session()

        for config_var in ("profile", "region"):
            if config_var in config:
                session.set_config_variable(config_var, config[config_var])

        if (
            config["aws_access_key_id"]
            or config["aws_secret_access_key"]
            or config["aws_session_token"]
        ):
            session.set_credentials(
                config["aws_access_key_id"],
                config["aws_secret_access_key"],
                config["aws_session_token"],
            )

        return session


session_factory = SessionFactory("AWS_CREDENTIALS")
get_session = session_factory.create


def create_client(service_name: str, config_name: str = None, **client_args):
    """
    Factory for creating AWS clients.
    """
    session = session_factory.create(config_name)
    return session.create_client(service_name, **client_args)
