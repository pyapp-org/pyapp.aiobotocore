from datetime import datetime
from typing import List, Set, Any, Optional
from uuid import UUID
from yarl import URL

from .base import Attribute, DataType
from .exceptions import ValidationError


class StringAttribute(Attribute[str]):
    """
    String attribute
    """

    dynamo_type = DataType.String

    def clean_value(self, value: Any) -> Optional[str]:
        if value is None or isinstance(value, str):
            return value
        return str(value)

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

    def clean_value(self, value: Any) -> Optional[int]:
        if value is None or isinstance(value, int):
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            raise ValidationError("Invalid integer")

    def prepare(self, value: int) -> str:
        return str(value)


class FloatAttribute(Attribute[float]):
    """
    Float attribute
    """

    dynamo_type = DataType.Number

    def clean_value(self, value: Any) -> Optional[float]:
        if value is None or isinstance(value, float):
            return value
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValidationError("Invalid float")

    def prepare(self, value: float) -> str:
        return str(value)


class BooleanAttribute(Attribute[bool]):
    """
    Boolean Attribute
    """

    dynamo_type = DataType.Bool

    def clean_value(self, value: Any) -> Optional[float]:
        if value is None:
            return None

        if value in (True, False):
            # if value is 1 or 0 then it's equal to True or False, but we want
            # to return a true bool for semantic reasons.
            return bool(value)

        raise ValidationError("Invalid bool")


class DateTimeAttribute(Attribute[datetime]):
    """
    Date/Time attribute
    """

    dynamo_type = DataType.String

    def clean_value(self, value: Any) -> Optional[datetime]:
        if value is None or isinstance(value, datetime):
            return value

        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValidationError("Invalid date-time")

    def prepare(self, value: datetime) -> str:
        return value.isoformat()


class UUIDAttribute(Attribute[UUID]):
    """
    UUID attribute
    """

    dynamo_type = DataType.String

    def clean_value(self, value: Any) -> Optional[UUID]:
        if value is None or isinstance(value, UUID):
            return value

        try:
            return UUID(value)
        except (TypeError, ValueError):
            raise ValidationError("Invalid UUID")

    def prepare(self, value: UUID) -> str:
        return str(value)


class URLAttribute(Attribute[URL]):
    """
    URL attribute
    """

    dynamo_type = DataType.String

    def clean_value(self, value: Any) -> Optional[URL]:
        if value is None or isinstance(value, URL):
            return value

        try:
            return URL(value)
        except (TypeError, ValueError):
            raise ValidationError("Invalid URL")

    def prepare(self, value: URL) -> str:
        return str(value)


class StringSetAttribute(StringAttribute):
    """
    String Set Attribute
    """

    dynamo_type = DataType.StringSet

    def clean_value(self, value: Any) -> Optional[set]:
        if value is None:
            return set()

        if isinstance(value, set):
            # TODO: Validate items
            return value

        raise ValidationError("Not a set")

    def prepare(self, value: Set[str]) -> List[str]:
        super_prepare = super().prepare
        return list(super_prepare(v) for v in value)


class BinarySetAttribute(BinaryAttribute):
    """
    Binary Set Attribute
    """

    dynamo_type = DataType.BinarySet

    def clean_value(self, value: Any) -> Optional[set]:
        if value is None:
            return set()

        if isinstance(value, set):
            # TODO: Validate items
            return value

        raise ValidationError("Not a set")

    def prepare(self, value: Set[bytes]) -> List[str]:
        super_prepare = super().prepare
        return list(super_prepare(v) for v in value)


class IntegerSetAttribute(IntegerAttribute):
    """
    Integer Set Attribute
    """

    dynamo_type = DataType.NumberSet

    def clean_value(self, value: Any) -> Optional[set]:
        if value is None:
            return set()

        if isinstance(value, set):
            # TODO: Validate items
            return value

        raise ValidationError("Not a set")

    def prepare(self, value: Set[int]) -> List[str]:
        super_prepare = super().prepare
        return list(super_prepare(v) for v in value)


class FloatSetAttribute(FloatAttribute):
    """
    Float Set Attribute
    """

    dynamo_type = DataType.NumberSet

    def clean_value(self, value: Any) -> Optional[set]:
        if value is None:
            return set()

        if isinstance(value, set):
            # TODO: Validate items
            return value

        raise ValidationError("Not a set")

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

    def clean_value(self, value: Any) -> Optional[list]:
        if value is None:
            return []

        if isinstance(value, list):
            # TODO: Validate items
            return value

        raise ValidationError("Not a list")

    def prepare(self, value: List[Any]) -> List[Any]:
        containing_prepare = self.containing_attribute.prepare
        return list(containing_prepare(v) for v in value)


SIMPLE_TYPES = {
    str: StringAttribute,
    bytes: BinaryAttribute,
    bool: BooleanAttribute,
    int: IntegerAttribute,
    float: FloatAttribute,
    datetime: DateTimeAttribute,
    UUID: UUIDAttribute,
    URL: URLAttribute,
}

SET_TYPES = {
    str: StringSetAttribute,
    bytes: BinarySetAttribute,
    int: IntegerSetAttribute,
    float: FloatSetAttribute,
}
