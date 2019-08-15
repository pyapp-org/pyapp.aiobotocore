from typing import Tuple, Type

from .base import Table
from .constants import BillingMode, ReturnValues
from .utils import get_attributes, to_dynamo, to_dynamo_key, clean as clean_
from ..factory import create_client


class Session:
    """
    Session object that wraps a DynamoDB async client
    """

    @classmethod
    def get(cls, *, config_name: str = None):
        return cls(create_client("dynamodb", config_name))

    def __init__(self, client):
        self.client = client

    async def create_table(self, table: Type[Table], provisioned_throughput: Tuple[int, int] = None):
        """
        Create a table.
        """
        attrs = get_attributes(table)

        kwargs = {
            "AttributeDefinitions": [a.attribute_def for a in attrs if a.key_type],
            "TableName": table.__name__,
            "KeySchema": [a.key_schema for a in attrs if a.key_type],
        }

        # Set the correct previsioned details
        if provisioned_throughput is None:
            kwargs["BillingMode"] = BillingMode.PayPerRequest.value
        else:
            kwargs["BillingMode"] = BillingMode.Provisioned.value
            read_cap, write_cap = provisioned_throughput
            kwargs["ProvisionedThroughput"] = {
                "ReadCapacityUnits": read_cap,
                "WriteCapacityUnits": write_cap,
            }

        return await self.client.create_table(**kwargs)

    async def put_item(self, item: Table, *, clean: bool = True, expression = None):
        """
        Store an item
        """
        if clean:
            await clean_(item)

        document = to_dynamo(item)
        return await self.client.put_item(
            Table_name=item.__table_name__,
            Item=document,
            ReturnValues=ReturnValues.ReturnNone
        )

    # def update_item(self, item: Table, expression = None):
    #     """
    #     Update an item
    #     """
    #     key = to_dynamo_key(item)
    #     document = to_dynamo(item, updates_only=True)
    #
    #     return self.client.update_item(
    #         TableName=item.__table_name__,
    #         Key=key,
    #         AttributeUpdates=document
    #     )

    async def delete_item(self, item: Table, expression = None):
        """
        Delete a table item.
        """
        key = to_dynamo_key(item)
        return await self.client.delete_item(
            TableName=item.__table_name__,
            Key=key,
            ConditionExpression=None,
            ExpressionAttributeValues=None,
            ReturnValues=ReturnValues.ReturnNone
        )
