from collections import OrderedDict
from enum import Enum
from typing import TypeVar, Generic, Type, Optional, Dict, Any, Sequence, Callable


class NoDefaultType:
    def __repr__(self):
        return "<NoDefault>"


NoDefault = NoDefaultType()


class DataType(Enum):
    """
    Data types supported by DynamoDB
    """

    Number = "N"
    String = "S"
    Bytes = "B"
    Bool = "BOOL"
    List = "L"
    Map = "M"
    NumberSet = "NS"
    StringSet = "SS"
    BytesSet = "BS"


IndexDataTypes = (DataType.Number, DataType.String, DataType.Bytes)


class KeyType(Enum):
    """
    Key types supported by DynamoDB
    """

    Hash = "HASH"
    Range = "RANGE"


class BillingMode(Enum):
    """
    Billing mode
    """
    Provisioned = "PROVISIONED"
    PayPerRequest = "PAY_PER_REQUEST"


VT_ = TypeVar("VT_")


class Attribute(Generic[VT_]):
    """
    A generic attribute
    """

    python_type: Type
    dynamo_type: DataType

    def __init__(
        self,
        name: str = None,
        *,
        key_type: KeyType = None,
        attr_name: str = None,
        default: VT_ = NoDefault,
        validators: Sequence[Callable[[VT_], None]]
    ):
        self.name = name
        self.key_type = key_type
        self.attr_name = attr_name
        self.default = default
        self.validators = validators

        # Ensure only data types that can be indexed are used if
        # an this attribute is a key field.
        if key_type and self.dynamo_type not in IndexDataTypes:
            raise RuntimeError(f"Only {IndexDataTypes} types can be indexed")

    def set_attrs_from_name(self, attr_name: str):
        """
        Apply the attribute name to Attribute instance
        """
        self.attr_name = attr_name
        self.name = self.name or attr_name

    def add_to_table(self, attr_name: str, klass: "Table"):
        """
        Called by the table meta class to apply name to object
        """
        self.set_attrs_from_name(attr_name)
        klass.__attributes__.append(self)

    @property
    def key_schema(self) -> Optional[Dict[str, str]]:
        """
        KeySchema entry for this attribute; returns `None` if this is not a key attribute
        """
        if self.key_type:
            return {"AttributeName": self.name, "KeyType": self.key_type.value}

    @property
    def attribute_def(self) -> Dict[str, str]:
        """
        AttributeDefinition entry for this attribute.
        """
        return {"AttributeName": self.name, "AttributeType": self.dynamo_type.value}

    def prepare(self, value: VT_) -> VT_:
        """
        Prepare an item
        """
        return value

    def to_dynamo(self, value: Optional[VT_]) -> Dict[str, Any]:
        """
        Prepare a value for storage in DynamoDB
        """
        if value is None:
            return {"NULL": True}
        else:
            return {self.dynamo_type.value: self.prepare(value)}


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
        new_attrs["__attributes__"] = []

        # Identify attributes that are fields
        attributes = []
        for name, value in attrs.items():
            if name in new_attrs:
                pass
            elif isinstance(value, Attribute):
                attributes.append((name, value))
            elif name in annotations:
                attributes.append((name, Attribute(default=value)))
            else:
                # Just a normal function or variable
                new_attrs[name] = value

        klass = super_new(mcs, class_name, bases, new_attrs)

        # Append to parent class (via options)
        for name, value in attributes:
            value.add_to_table(name, annotations.get(name), klass)

        return klass


class Table(metaclass=TableMeta):
    """
    Table base class
    """
