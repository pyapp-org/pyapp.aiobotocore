import asyncio
import botocore.exceptions
import logging

from pyapp.injection import inject
from pyapp_ext.messaging.asyncio import bases
from pyapp_ext.messaging.exceptions import QueueNotFound
from typing import Optional, Dict, Any

from .factory import create_client

__all__ = ("SQSSender", "SQSReceiver", "SNSSender")


logger = logging.getLogger(__file__)


def build_attributes(**attrs):
    attributes = {}
    for key, value in attrs.items():
        if value is not None:
            attributes[key] = {"DataType": "String", "StringValue": value}
    return attributes


def parse_attributes(attributes):
    attrs = {}
    for key, value in attributes.items():
        attrs[key] = value["StringValue"]
    return attrs


class SQSBase:
    """
    Base Message Queue
    """

    __slots__ = (
        "queue_name",
        "aws_config",
        "client_args",
        "_client",
        "_queue_url",
        "loop",
    )

    @inject
    def __init__(
        self,
        *,
        queue_name: str,
        aws_config: str = None,
        client_args: Dict[str, Any] = None,
        loop: asyncio.AbstractEventLoop = None,
    ):
        self.queue_name = queue_name
        self.aws_config = aws_config
        self.client_args = client_args or {}
        self.loop = loop

        self._client = None
        self._queue_url: Optional[str] = None

    async def open(self):
        """
        Open queue
        """
        client = create_client("sqs", self.aws_config, **self.client_args)

        try:
            response = await client.get_queue_url(QueueName=self.queue_name)
        except botocore.exceptions.ClientError as err:
            await client.close()
            error_code = err.response["Error"]["Code"]
            if error_code == "AWS.SimpleQueueService.NonExistentQueue":
                raise QueueNotFound(f"Unable to find queue `{self.queue_name}`")
            else:
                raise

        self._client = client
        self._queue_url = response["QueueUrl"]

    async def close(self):
        """
        Close Queue
        """
        if self._client:
            await self._client.close()
            self._client = None

        self._queue_url = None

    async def configure(self):
        """
        Define any send queues
        """
        client = create_client("sqs", self.aws_config, **self.client_args)

        try:
            response = await client.create_queue(QueueName=self.queue_name)
        except botocore.exceptions.ClientError as err:
            error_code = err.response["Error"]["Code"]
            raise
        else:
            return response["QueueUrl"]
        finally:
            await client.close()


class SQSSender(SQSBase, bases.MessageSender):
    """
    AIO SQS message sender.
    """

    __slots__ = ()

    async def send_raw(
        self, body: bytes, *, content_type: str = None, content_encoding: str = None
    ) -> str:
        """
        Publish a raw message (message is raw bytes)
        """
        attributes = build_attributes(
            ContentType=content_type, ContentEncoding=content_type
        )
        response = await self._client.send_message(
            QueueUrl=self._queue_url, MessageBody=body, MessageAttributes=attributes
        )
        return response["MessageId"]


class SQSReceiver(SQSBase, bases.MessageReceiver):
    """
    AIO SQS message receiver/subscriber
    """

    __slots__ = ("_listen_task",)

    _listen_task: Optional[asyncio.Task]

    async def _listener(self):
        """
        Internal listener task
        """
        client = self._client
        queue_url = self._queue_url

        logger.debug("Starting SQS Listener")

        while True:
            try:
                response = await client.receive_message(
                    QueueUrl=queue_url,
                    WaitTimeSeconds=10,
                    MessageAttributeNames=["ContentType", "ContentEncoding"],
                )

                if "Messages" in response:
                    for msg in response["Messages"]:
                        try:
                            raw_attrs = msg["MessageAttributes"]
                        except KeyError:
                            attrs = {}
                        else:
                            attrs = parse_attributes(raw_attrs)

                        await self.receive(
                            msg["Body"],
                            attrs.get("ContentType"),
                            attrs.get("ContentEncoding"),
                        )
                        await client.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
                        )
                else:
                    logger.debug("No messages in queue")

            except botocore.exceptions.ClientError:
                raise

    async def open(self):
        """
        Listen for messages.
        """
        await super().open()
        self._listen_task = self.loop.create_task(self._listener())

    async def close(self):
        """
        Stop listening for messages
        """
        self._listen_task.cancel()
        await super().close()


class SNSBase:
    """
    Base Pub/Sub Queue
    """

    __slots__ = ("topic_arn", "aws_config", "client_args", "_client")

    def __init__(
        self, topic_arn: str, aws_config: str = None, client_args: Dict[str, Any] = None
    ):
        self.topic_arn = topic_arn
        self.aws_config = aws_config
        self.client_args = client_args or {}

        self._client = None

    async def open(self):
        """
        Open queue
        """
        client = create_client("sns", self.aws_config, **self.client_args)

        self._client = client

    async def close(self):
        """
        Close Queue
        """
        if self._client:
            await self._client.close()
            self._client = None


class SNSSender(SNSBase, bases.MessageSender):
    """
    AIO SNS message publisher.
    """

    __slots__ = ()

    async def send_raw(
        self, body: bytes, *, content_type: str = None, content_encoding: str = None
    ) -> str:
        attributes = build_attributes(
            ContentType=content_type, ContentEncoding=content_type
        )
        response = self._client.publish(
            TopicArn=self.topic_arn, Message=body, MessageAttributes=attributes
        )
        return response["MessageId"]
