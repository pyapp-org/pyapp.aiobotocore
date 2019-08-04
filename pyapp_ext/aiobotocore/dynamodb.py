import abc
import uuid
from abc import ABC

from collections import OrderedDict
from datetime import datetime
from typing import (
    Tuple,
    Callable,
    Generic,
    Sequence,
    Set,
    Any,
    _GenericAlias,
    List,
    Optional,
    TypeVar,
    Dict,
    Type,
)

BasicType = Tuple[str, Callable, Callable]
VT_ = TypeVar("VT_", bytes, str, int, float, bool, datetime, set, list, dict)
ST_ = TypeVar("ST_", bytes, str, int, float)


class Field(Generic[VT_], abc.ABC):
    """
    Base field.
    """

    dynamo_type: str
    python_type: Type

    def __init__(self, name: str = None):
        self.name = name
        self.attr_name: Optional[str] = None

    def set_attributes_from_name(self, name: str):
        self.attr_name = name
        self.name = self.name or name

    def add_to_table(self, name: str, klass):
        self.set_attributes_from_name(name)
        klass.__fields__.append(self)

    @abc.abstractmethod
    def to_python(self, value: Any) -> Optional[VT_]:
        pass

    @abc.abstractmethod
    def prepare(self, value: Optional[VT_]) -> Any:
        pass

    def to_dynamo(self, value: Optional[VT_]) -> Dict[str, Any]:
        """
        Generate the DynamoDB value
        """
        if value is None:
            return {"NULL": True}
        return {self.dynamo_type: self.prepare(value)}


class BytesField(Field[bytes]):
    dynamo_type = "B"
    python_type = bytes

    def to_python(self, value: Any) -> bytes:
        pass

    def prepare(self, value: Optional[bytes]) -> Any:
        pass


class StringField(Field[str]):
    dynamo_type = "S"
    python_type = str

    def to_python(self, value: Any) -> str:
        pass

    def prepare(self, value: Optional[str]) -> Any:
        pass


class BooleanField(Field):
    dynamo_type = "BOOL"
    python_type = bool


class NumberField(Field, ABC):
    dynamo_type = "N"

    def prepare(self, value: VT_) -> Any:
        return str(value)


class IntegerField(NumberField):
    python_type = int


class FloatField(NumberField):
    python_type = float


class ListField(Field):
    dynamo_type = "L"
    python_type = list


class SetField(Generic[ST_], Field[Set[ST_]], ABC):
    python_type = set
    set_item_field: Field

    def prepare(self, value: Set[ST_]) -> List[Dict[str, ST_]]:
        to_dynamo = self.set_item_field.to_dynamo
        return list(to_dynamo(v) for v in value)


class BinarySetField(SetField[bytes]):
    dynamo_type = "BS"
    set_item_field = BytesField()

    def to_python(self, value: Any) -> Optional[VT_]:
        pass


class StringSetField(SetField):
    dynamo_type = "SS"


class NumberSetField(SetField):
    dynamo_type = "NS"


class IntegerSetField(NumberSetField):
    pass


class FloatSetField(NumberSetField):
    pass


class MapField(Field):
    dynamo_type = "M"


class TableMeta(type):
    """
    Meta class to automate the creation of Table classes.
    """

    @classmethod
    def __prepare__(mcs, name, bases):
        return OrderedDict()

    def __new__(mcs, class_name, bases, attrs, name: str = None):
        super_new = super().__new__
        new_attrs = {k: v for k, v in attrs.items() if k.startswith("__")}
        annotations = attrs.get("__annotations__", {})

        # Determine the name of the table
        new_attrs["__tablename__"] = name or attrs.get("__tablename__", class_name)
        new_attrs["__fields__"] = []

        # Identify attributes that are fields
        fields = []
        for name, value in attrs.items():
            if name in new_attrs:
                pass
            elif isinstance(value, Options):
                fields.append((name, value))
            elif name in annotations:
                fields.append((name, Options(default=value)))
            else:
                # Just a normal function or variable
                new_attrs[name] = value

        klass = super_new(mcs, class_name, bases, new_attrs)

        # Append to parent class (via options)
        for name, value in fields:
            value.add_to_table(name, annotations.get(name), klass)

        return klass


class Options:
    def __init__(
        self,
        *,
        primary_key: bool = False,
        sort_key: bool = False,
        name: str = None,
        default: Any = None,
        python_type: Any = None,
    ):
        self.primary_key = primary_key
        self.sort_key = sort_key
        self.name = name

        self.attr_name: Optional[str] = None

    def set_name(self, attr_name: str):
        if not self.name:
            self.name = attr_name
        self.attr_name = attr_name

    def add_to_table(self, attr_name: str, python_type: type, klass):
        self.set_name(attr_name)


class Table(metaclass=TableMeta, name="MyTable"):
    name: str = Options(primary_key=True)
    age: int = Options(sort_key=True)
    children: List[str]
    favourite_colours: Set[str]


table = Table()


# Mapping of basic data type to DynamoDB
BASIC_TYPES: Sequence[Tuple[type, BasicType]] = (
    (str, ("S", None, None)),
    (bytes, ("B", None, None)),
    (bool, ("BOOL", None, bool)),
    (int, ("N", None, int)),
    (float, ("N", None, float)),
    (list, ("L", None, None)),
    (dict, ("M", None, None)),
    (uuid.UUID, ("S", str, uuid.UUID)),
    (datetime, ("N", datetime.timestamp, datetime.fromtimestamp)),
)
# Type supported as sets
SET_TYPES = {"S", "B", "N"}
# Mapping for None
NONE_TYPE = "NULL"


def _resolve_basic_type(obj: Any) -> BasicType:
    check_func = issubclass if isinstance(obj, type) else isinstance

    for lt, bt in BASIC_TYPES:
        if check_func(obj, lt):
            return bt

    raise TypeError(f"{obj!r} not supported by DynamoDB")


def resolve_dynamo_type(obj: Any) -> BasicType:
    """
    Resolve a type to a DynamoDB definition
    """
    # Handle generic aliases (types using subscripting)
    if isinstance(obj, _GenericAlias):
        if isinstance(obj.__origin__, set):
            set_type, = obj.__args__
            basic_type = _resolve_basic_type(set_type)

    else:
        return _resolve_basic_type(obj)
