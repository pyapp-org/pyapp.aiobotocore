import botocore.exceptions

from typing import Tuple, Type, Sequence, Dict, Any

from .base import Table, Attribute
from .constants import BillingMode, ReturnValues, Select, KeyType
from .exceptions import TableNotFound
from .utils import get_attributes, to_dynamo, to_dynamo_key, clean as clean_, key_attributes
from ..factory import create_client


class Filter:
    def __init__(self, session: "Session", table: Type[Table], kwargs: Dict[str, Any] = None):
        self.session = session
        self.table = table
        self.kwargs = kwargs or {}

    async def __aiter__(self):
        result = await self._execute(**self.kwargs)
        for item in result["Items"]:
            yield item

    async def _execute(self, **kwargs):
        raise NotImplementedError

    async def count(self):
        """
        Return the count of the current filter
        """
        result = await self._execute(Select=Select.Count, **self.kwargs)
        return result["Count"]

    def limit(self, value: int):
        """
        The maximum number of items to evaluate.
        """
        kwargs = self.kwargs.copy()
        kwargs["Limit"] = value
        return type(self)(self.session, self.table, kwargs)

    def consistent(self, value: bool = True):
        """
        Do a consistent read
        """
        kwargs = self.kwargs.copy()
        kwargs["ConsistentRead"] = value
        return type(self)(self.session, self.table, kwargs)


class Scan(Filter):
    async def _execute(self, **kwargs):
        try:
            return await self.session.client.scan(
                TableName=self.table.__table_name__,
                **kwargs
            )
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "ResourceNotFoundException":
                raise TableNotFound(f"{self.table.__table_name__} not found.")
            else:
                raise


class Query(Filter):
    async def _execute(self, **kwargs):
        try:
            return await self.session.client.query(
                TableName=self.table.__table_name__,
                **kwargs
            )
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "ResourceNotFoundException":
                raise TableNotFound(f"{self.table.__table_name__} not found.")
            else:
                raise


class Session:
    """
    Session object that wraps a DynamoDB async client
    """

    @classmethod
    def get(cls, *, config_name: str = None):
        return cls(create_client("dynamodb", config_name))

    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """
        Ensure AIOBotoCore session is closed
        """
        if self.client:
            await self.client.close()
            self.client = None

    async def describe_table(self, table_name: str):
        """
        Obtain the description of a table.
        """
        try:
            result = await self.client.describe_table(TableName=table_name)
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "ResourceNotFoundException":
                raise TableNotFound(f"{table_name} not found.")
        else:
            return result["Table"]

    async def create_table(
        self,
        table: Type[Table],
        *,
        provisioned_throughput: Tuple[int, int] = None,
        check_exists: bool = True
    ):
        """
        Create a table.
        """
        if check_exists:
            try:
                return await self.describe_table(table.__table_name__)
            except TableNotFound:
                pass

        attrs = get_attributes(table)

        kwargs = {
            "AttributeDefinitions": [a.attribute_def for a in attrs if a.key_type],
            "TableName": table.__table_name__,
            "KeySchema": [a.key_schema for a in attrs if a.key_type],
        }

        # Set the correct previsioned details
        if provisioned_throughput is None:
            kwargs["BillingMode"] = BillingMode.PayPerRequest
        else:
            kwargs["BillingMode"] = BillingMode.Provisioned
            read_cap, write_cap = provisioned_throughput
            kwargs["ProvisionedThroughput"] = {
                "ReadCapacityUnits": read_cap,
                "WriteCapacityUnits": write_cap,
            }

        try:
            result = await self.client.create_table(**kwargs)
        except botocore.exceptions.ClientError:
            raise  # TODO
        else:
            return result["TableDescription"]

    async def put_item(self, item: Table, *, clean: bool = True, expression=None):
        """
        Store an item
        """
        if clean:
            await clean_(item)

        document = to_dynamo(item)
        try:
            await self.client.put_item(
                TableName=item.__table_name__,
                Item=document,
                ReturnValues=ReturnValues.ReturnNone,
            )
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "ResourceNotFoundException":
                raise TableNotFound(f"{item.__table_name__} not found.")
            else:
                raise

    async def get_item(self, table: Type[Table], key, *, projection: Sequence[Attribute] = None, consistent: bool = False):
        """
        Get an item from Dynamo
        """
        keys = key_attributes(table)

    def scan(self, table: Type[Table]) -> Scan:
        """
        Generate a scan expression
        """
        return Scan(self, table)

    def query(self, table: Type[Table]) -> Query:
        """
        Generate a query expression
        """
        return Query(self, table)

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

    async def delete_item(self, item: Table, expression=None):
        """
        Delete a table item.
        """
        key = to_dynamo_key(item)
        try:
            await self.client.delete_item(
                TableName=item.__table_name__,
                Key=key,
                ConditionExpression=None,
                ExpressionAttributeValues=None,
                ReturnValues=ReturnValues.ReturnNone,
            )
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "ResourceNotFoundException":
                pass
            else:
                raise
