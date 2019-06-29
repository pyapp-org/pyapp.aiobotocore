import pytest

from pyapp.conf import settings

from pyapp_ext.aiobotocore import factory


class TestSessionFactory:
    def test_session_factory(self):
        session_factory = factory.SessionFactory('AWS_CREDENTIALS')
        session = session_factory.create()

        assert session.profile is None
        assert session._session_instance_vars == {}

    def test_session_factory__region_name(self):
        with settings.modify() as context:
            context.AWS_CREDENTIALS = {
                "default": {"region_name": "foo"}
            }
            session_factory = factory.SessionFactory('AWS_CREDENTIALS')
            session = session_factory.create()

        assert session._session_instance_vars['region'] == "foo"

    @pytest.mark.parametrize("credential, expected", (
        ("aws_access_key_id", "access_key"),
        ("aws_secret_access_key", "secret_key"),
        ("aws_session_token", "token"),
    ))
    def test_session_factory__credentials(self, credential, expected):
        with settings.modify() as context:
            context.AWS_CREDENTIALS = {
                "default": {credential: "foo"}
            }
            session_factory = factory.SessionFactory("AWS_CREDENTIALS")
            session = session_factory.create()

        assert getattr(session._credentials, expected) == "foo"
