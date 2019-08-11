from typing import List, Set, Any

import datetime
import uuid
import yarl

from .base import Attribute, DataType


class StringAttribute(Attribute[str]):
    """
    String attribute
    """

    dynamo_type = DataType.String

    def prepare(self, value: str) -> str:
        return str(value)


class BinaryAttribute(Attribute[bytes]):
    """
    Binary attribute
    """

    dynamo_type = DataType.Binary

    def prepare(self, value: bytes) -> str:
        return value.hex()


class IntegerAttribute(Attribute[int]):
    """
    Integer attribute
    """

    dynamo_type = DataType.Number

    def prepare(self, value: int) -> str:
        return str(value)


class FloatAttribute(Attribute[float]):
    """
    Float attribute
    """

    dynamo_type = DataType.Number

    def prepare(self, value: float) -> str:
        return str(value)


class BooleanAttribute(Attribute[bool]):
    """
    Boolean Attribute
    """

    dynamo_type = DataType.Bool


class DateTimeAttribute(Attribute[datetime.datetime]):
    """
    Date/Time attribute
    """

    dynamo_type = DataType.String

    def prepare(self, value: datetime.datetime) -> str:
        return value.isoformat()


class UUIDAttribute(Attribute[uuid.UUID]):
    """
    UUID attribute
    """

    dynamo_type = DataType.String

    def prepare(self, value: uuid.UUID) -> str:
        return str(value)


class URLAttribute(Attribute[yarl.URL]):
    """
    URL attribute
    """

    dynamo_type = DataType.String

    def prepare(self, value: yarl.URL) -> str:
        return str(value)


class StringSetAttribute(StringAttribute):
    """
    String Set Attribute
    """

    dynamo_type = DataType.StringSet

    def prepare(self, value: Set[str]) -> List[str]:
        super_prepare = super().prepare
        return list(super_prepare(v) for v in value)


class BinarySetAttribute(BinaryAttribute):
    """
    Binary Set Attribute
    """

    dynamo_type = DataType.BinarySet

    def prepare(self, value: Set[bytes]) -> List[str]:
        super_prepare = super().prepare
        return list(super_prepare(v) for v in value)


class IntegerSetAttribute(IntegerAttribute):
    """
    Integer Set Attribute
    """

    dynamo_type = DataType.NumberSet

    def prepare(self, value: Set[int]) -> List[str]:
        super_prepare = super().prepare
        return list(super_prepare(v) for v in value)


class FloatSetAttribute(FloatAttribute):
    """
    Float Set Attribute
    """

    dynamo_type = DataType.NumberSet

    def prepare(self, value: Set[float]) -> List[str]:
        super_prepare = super().prepare
        return list(super_prepare(v) for v in value)


class ListAttribute(Attribute):
    """
    List Attribute
    """

    dynamo_type = DataType.List

    def __init__(self, containing_attribute: Attribute, name: str = None, **kwargs):
        super().__init__(name, **kwargs)
        self.containing_attribute = containing_attribute

    def prepare(self, value: List[Any]) -> List[Any]:
        containing_prepare = self.containing_attribute.prepare
        return list(containing_prepare(v) for v in value)


SIMPLE_TYPES = {
    str: StringAttribute,
    bytes: BinaryAttribute,
    bool: BooleanAttribute,
    int: IntegerAttribute,
    float: FloatAttribute,
    datetime.datetime: DateTimeAttribute,
    uuid.UUID: UUIDAttribute,
    yarl.URL: URLAttribute,
}

SET_TYPES = {
    str: StringSetAttribute,
    bytes: BinarySetAttribute,
    int: IntegerSetAttribute,
    float: FloatSetAttribute,
}
