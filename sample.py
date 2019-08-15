import aiobotocore
import asyncio

from datetime import datetime
from pprint import pprint
from uuid import uuid4

from pyapp_ext.aiobotocore import dynamodb as ddb


class Record(ddb.Table, name="TestRecord"):
    id = ddb.UUIDAttribute(key_type=ddb.KeyType.Hash, default_factory=uuid4)
    created = ddb.DateTimeAttribute(
        key_type=ddb.KeyType.Range, default_factory=datetime.now
    )
    age = ddb.IntegerAttribute()


async def main():
    async with ddb.Session(
        client=aiobotocore.get_session().create_client(
            "dynamodb",
            region_name="woo!",
            endpoint_url="http://localhost:8324"
        )
    ) as session:

        # await session.client.delete_table(TableName=Record.__table_name__)
        result = await session.create_table(Record)
        pprint(result)

        results = await session.scan(Record).count()
        pprint(results)

        async for item in session.query(Record).consistent():
            pprint(item)

        # record = Record(age=10)
        # await session.put_item(record)




if __name__ == "__main__":
    asyncio.run(main())
