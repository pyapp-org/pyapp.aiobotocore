AWS_CREDENTIALS = {"default": {}}
"""
Named sets of credentials for AWS.

Example::

    AWS_CREDENTIALS = {"default": {
        "region_name": None,
        "aws_access_key_id": None,
        "aws_secret_access_key": None,
        "aws_session_token": None,    
    }}

"""

AWS_CLIENTS = {}
"""
AWS client configuration.

Example::

    AWS = {
        "S3": {
            "incoming": {
                "credentials: "default",
                "BucketName": "myapp.incoming",
            }
        },
        "SQS": {
            "job_queue": {
                "url": "",
            }
        }
    }

"""