import asyncio

from datetime import datetime
from uuid import uuid4

from pyapp_ext.aiobotocore import dynamodb as ddb


class Record(ddb.Table, name="Record Name"):
    id = ddb.UUIDAttribute(key_type=ddb.KeyType.Hash, default_factory=uuid4)
    created = ddb.DateTimeAttribute(
        key_type=ddb.KeyType.Range, default_factory=datetime.now
    )
    age = ddb.IntegerAttribute()


async def main():
    record = Record(age=10)
    await ddb.clean(record)

    ddb.get_attributes(record)


if __name__ == "__main__":
    asyncio.run(main())
