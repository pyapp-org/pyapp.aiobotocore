from typing import Tuple, Type

from .base import Table
from .constants import BillingMode, ReturnValues
from .utils import get_attributes, to_dynamo, to_dynamo_key
from ..factory import create_client


class Session:
    """

    """
    def __init__(self, *, config_name: str = None):
        self.client = create_client("dynamodb", config_name)

    def create_table(self, table: Type[Table], provisioned_throughput: Tuple[int, int] = None):
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

        return self.client.create_table(**kwargs)

    def put_item(self, item: Table, expression = None):
        """
        Store an item
        """
        document = to_dynamo(item)

        return self.client.put_item(
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

    def delete_item(self, item: Table, expression = None):
        """
        Delete a table item.
        """
        key = to_dynamo_key(item)
        return self.client.delete_item(
            TableName=item.__table_name__,
            Key=key,
            ConditionExpression=None,
            ExpressionAttributeValues=None,
            ReturnValues=ReturnValues.ReturnNone
        )
