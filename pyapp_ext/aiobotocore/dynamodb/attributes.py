import uuid
import datetime
import yarl

from .base import Attribute, DataType


class StringAttribute(Attribute[str]):
    """
    String attribute
    """
    python_type = str
    dynamo_type = DataType.String

    def prepare(self, value: str) -> str:
        return str(value)


class BytesAttribute(Attribute[bytes]):
    """
    String attribute
    """
    python_type = bytes
    dynamo_type = DataType.Bytes

    def prepare(self, value: bytes) -> str:
        return value.hex()


class IntegerAttribute(Attribute[int]):
    """
    Integer attribute
    """
    python_type = int
    dynamo_type = DataType.Number

    def prepare(self, value: int) -> str:
        return str(value)


class FloatAttribute(Attribute[float]):
    """
    Float attribute
    """
    python_type = float
    dynamo_type = DataType.Number

    def prepare(self, value: float) -> str:
        return str(value)


class BooleanAttribute(Attribute[bool]):
    """
    Boolean Attribute
    """
    python_type = bool
    dynamo_type = DataType.Bool


class DateTimeAttribute(Attribute[datetime.datetime]):
    """
    Date/Time attribute
    """
    python_type = datetime.datetime
    dynamo_type = DataType.String

    def prepare(self, value: datetime.datetime) -> str:
        return value.isoformat()


class UUIDAttribute(Attribute[uuid.UUID]):
    """
    UUID attribute
    """
    python_type = uuid.UUID
    dynamo_type = DataType.String

    def prepare(self, value: uuid.UUID) -> str:
        return str(value)


class URLAttribute(Attribute[yarl.URL]):
    """
    URL attribute
    """
    python_type = yarl.URL
    dynamo_type = DataType.String

    def prepare(self, value: yarl.URL) -> str:
        return str(value)


SIMPLE_TYPES = {
    str: StringAttribute,
    bytes: BytesAttribute,
    bool: BooleanAttribute,
    int: IntegerAttribute,
    float: FloatAttribute,
    datetime.datetime: DateTimeAttribute,
    uuid.UUID: UUIDAttribute,
    yarl.URL: URLAttribute
}
