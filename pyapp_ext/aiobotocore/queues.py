import botocore.exceptions
import logging

from typing import Optional, Dict, Any

from pyapp_ext.messaging.asyncio import bases
from pyapp_ext.messaging.exceptions import QueueNotFound

from .factory import create_client

__all__ = ("MessageSender", "MessageReceiver")


logger = logging.getLogger(__file__)


class SQSBase:
    """
    Base Message Queue
    """

    __slots__ = ("queue_name", "aws_config", "client_args", "_client", "_queue_url")

    def __init__(
        self,
        queue_name: str,
        aws_config: str = None,
        client_args: Dict[str, Any] = None,
    ):
        self.queue_name = queue_name
        self.aws_config = aws_config
        self.client_args = client_args or {}

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
            if (
                err.response["Error"]["Code"]
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
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


class MessageSender(SQSBase, bases.MessageSender):
    """
    AIO Pika based message sender/publisher.

    With AMQP senders and publishers are the same.

    """

    __slots__ = ()

    async def send_raw(
        self, body: bytes, *, content_type: str = None, content_encoding: str = None
    ):
        """
        Publish a raw message (message is raw bytes)
        """
        attributes = {}
        if content_type:
            attributes["ContentType"] = {
                "DataType": "string",
                "StringValue": content_type,
            }
        if content_encoding:
            attributes["ContentEncoding"] = {
                "DataType": "string",
                "StringValue": content_encoding,
            }

        await self._client.send_message(
            QueueUrl=self._queue_url, MessageBody=body,
            # MessageAttributes=attributes
        )


class MessageReceiver(SQSBase, bases.MessageReceiver):
    """
    AIO Pika based message receiver
    """

    __slots__ = ()

    async def listen(self):
        """
        Listen for messages.
        """
        client = self._client
        queue_url = self._queue_url

        while True:
            try:
                response = await client.receive_message(
                    QueueUrl=queue_url, WaitTimeSeconds=10
                )

                if "Messages" in response:
                    for msg in response["Messages"]:
                        await self.receive(msg["Body"])
                        await client.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
                        )
                else:
                    logger.debug("No messages in queue")

            except botocore.exceptions.ClientError:
                raise
