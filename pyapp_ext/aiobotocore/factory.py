import aiobotocore

from botocore.session import Session
from pyapp.conf import settings
from pyapp.conf.helpers import ThreadLocalNamedSingletonFactory
from typing import Dict, Any

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


class ClientFactory:
    """
    Factory that creates specific clients
    """

    def __init__(self, *, create_session=get_session):
        self.create_session = create_session
        self._cache = {}

    def __getitem__(self, service_name: str) -> Dict[str, Any]:
        service_name = service_name.upper()
        try:
            return self._cache[service_name]
        except KeyError:
            service_settings = getattr(settings, f"AWS_{service_name}", {})
            self._cache[service_name] = service_settings
            return service_settings

    def create(
        self,
        service_name: str,
        config_name: str = None,
        service_config_name: str = None,
        **client_args,
    ):
        """
        Create a service client

        :param service_name: Name of the AWS service to create a client for
        :param config_name: Name of the Boto config to
        :param service_config_name: Name of a service config that provides additional client args.
        :param client_args: Any additional arguments for the client (updates `service_config` args)

        """
        session = self.create_session(config_name)
        client_settings = self[service_name].get(service_config_name or "default", {})
        client_settings.update(client_args)
        return session.create_client(service_name, **client_settings)


client_factory = ClientFactory()
create_client = client_factory.create
