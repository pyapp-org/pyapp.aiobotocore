###################
pyApp - AIOBotocore
###################

*Let us handle the boring stuff!*

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/ambv/black
      :alt: Once you go Black...

.. image:: https://api.codeclimate.com/v1/badges/0a86755f39f0416fbd1e/maintainability
   :target: https://codeclimate.com/github/pyapp-org/pae.sqlalchemy/maintainability
   :alt: Maintainability

This extension provides `Session` and *client* factories for
`aiobotocore <https://github.com/aio-libs/aiobotocore>`_ to allow authorization
details to be configured via pyApp settings.

The extension also provides checks to confirm the settings are correct.

The extension now supports the standardised 
`pyApp-Messaging <https://github.com/pyapp-org/pyapp-messaging>`_ interface.

Installation
============

Install using *pip*::

    pip install pyapp-aiobotocore

Install using *pipenv*::

    pipenv install pyapp-aiobotocore


Optionally add the `AWS_CREDENTIALS` block into your runtime settings file, this
is only required if you need to apply specific credentials, the default settings
provided by this extension work with assumed roles within of the AWS environment::

    AWS_CREDENTIALS = {
        "default": {
            "region_name": None,
            "aws_access_key_id": None,
            "aws_secret_access_key": None,
            "aws_session_token": None,
        }
    }


Usage
=====

The following example obtains an S3 client::

    from pyapp_ext.aiobotocore import create_client

    s3 = create_client("S3")


API
===

`pyapp_ext.aiobotocore.create_client(service_name: str, *, credentials: str = None, **client_kwargs)`

    Get an async botocore service client instance.


`pyapp_ext.aiobotocore.get_session(default: str = None) -> Session`

    Get named `Session` instance.

    
