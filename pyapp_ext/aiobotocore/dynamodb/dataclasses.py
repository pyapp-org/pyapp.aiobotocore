"""
Dataclass Support
~~~~~~~~~~~~~~~~~

Use `dataclasses` to define DynamoDB items and tables.

This module provides additional decorators and utility functions that extend the
basic dataclass features with additional DynamoDB specific functionality.

The following methods are *optional* `asitem` will still convert a normal dataclass
into a DynamoDB structured `dict`.

**table** - Extends the `dataclass` decorator with the table name (this defaults to
    the name of the class.

**attribute** - Extends the `field` function to be able to define the name (defaults
    to he attribute name), optional Key and validators.

Example::

    @table
    class MyTable:
        name: str
        age: int
        tags: Set[str] = attribute(default_factory=set)
        id: UUID = attribute(key_type=KeyType.Hash, default_factory=uuid4)

    # Generate table description for use with create.
    table_desc = table_description(Record)

    # Prepare for adding to DynamoDB
    my_table = MyTable(name="foo", 1, {"a", "b"})
    item = asitem(my_table)

"""
from dataclasses import MISSING, Field, fields, is_dataclass, _process_class
from functools import partial
from typing import List, Callable, Any, Tuple, Dict, _GenericAlias, Type

from .base import Attribute
from .constants import KeyType, BillingMode, DataType
from .exceptions import DynamoDBError
from .attributes import SIMPLE_TYPES, SET_TYPES, ListAttribute

__all__ = (
    "table",
    "asitem",
    "fromitem",
    "attribute",
    "attributes",
    "UnsupportedType",
    "table_description",
)

DYNAMO_DB_ARGS_KEY = "_dynamo_db"


def table(
    _cls=None,
    *,
    name: str = None,
    # Standard dataclass kwargs
    init: bool = True,
    repr: bool = True,
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
):
    """
    A wrapper around `dataclass.dataclass` to provide additional DynamoDB specific information.
    """

    def wrap(cls):
        klass = _process_class(cls, init, repr, eq, order, unsafe_hash, frozen)
        klass.__table_name__ = name or cls.__name__
        klass.__attributes__ = tuple(_field_to_attribute(f) for f in fields(cls))
        return klass

    return wrap if _cls is None else wrap(_cls)


def attribute(
    *,
    name: str = None,
    key_type: KeyType = None,
    validators: List[Callable[[Any], None]] = None,
    # Standard field kwargs
    default=MISSING,
    default_factory=MISSING,
    init=True,
    repr=True,
    hash=None,
    compare=True,
    metadata=None,
):
    """
    A wrapper around `dataclass.field` to extend the `metadata` argument with
    DynamoDB specific information.
    """
    if default is not MISSING and default_factory is not MISSING:
        raise ValueError("cannot specify both default and default_factory")

    # Generate attribute args for DynamoDB
    attribute_args = {"key_type": key_type, "validators": validators}
    if name:
        attribute_args["name"] = name

    metadata = metadata or {}
    metadata[DYNAMO_DB_ARGS_KEY] = attribute_args
    return Field(default, default_factory, init, repr, hash, compare, metadata)


class DataclassAttribute(Attribute):
    """
    Attribute that wraps a dataclass
    """

    dynamo_type = DataType.Map

    def __init__(self, klass, name: str = None, **kwargs):
        super().__init__(name, **kwargs)

        self.klass = klass

    def prepare(self, value) -> Dict[str, Any]:
        return asitem(value)


class UnsupportedType(TypeError, DynamoDBError):
    """
    Type is not supported.
    """


def _type_to_attribute(type_: type):
    """
    Map a type back to an attribute instance
    """
    try:
        return SIMPLE_TYPES[type_]
    except KeyError:
        pass

    if is_dataclass(type_):
        return partial(DataclassAttribute, type_)

    if isinstance(type_, _GenericAlias):
        origin_type = type_.__origin__
        if origin_type is list:
            # List generic only supports a single argument
            contained_type, = type_.__args__
            contained_attribute = _type_to_attribute(contained_type)
            return partial(ListAttribute, contained_attribute())

        if origin_type is set:
            # Set generic only supports a single argument
            contained_type, = type_.__args__
            try:
                return SET_TYPES[contained_type]
            except KeyError:
                raise UnsupportedType(
                    f"Set type {contained_type!r} not supported"
                ) from None

    raise UnsupportedType(f"Type {type_!r} not supported")


def _field_to_attribute(field: Field) -> Attribute:
    """
    Get an attribute instance from a field definition.
    """
    attr_klass = _type_to_attribute(field.type)

    attr_kwargs = field.metadata.get(DYNAMO_DB_ARGS_KEY, {})
    attr_kwargs.setdefault("name", field.name)
    attr_kwargs.setdefault("attr_name", field.name)

    return attr_klass(**attr_kwargs)


def attributes(class_or_instance) -> Tuple[Attribute]:
    """
    Return a tuple describing the attributes of this dataclass.

    Accepts a dataclass or an instance of one. Tuple elements are of
    sub-type Attribute.
    """
    try:
        return getattr(class_or_instance, "__attributes__")
    except AttributeError:
        pass  # Ignore as we will now populate this value.

    attrs = tuple(_field_to_attribute(f) for f in fields(class_or_instance))
    # Apply to the class so is cached across all instances
    klass = (
        class_or_instance
        if isinstance(class_or_instance, type)
        else type(class_or_instance)
    )
    setattr(klass, "__attributes__", attrs)
    return attrs


def table_description(
    class_or_instance,
    *,
    table_name: str = None,
    provisioned_throughput: Tuple[int, int] = None,
) -> Dict[str, Any]:
    """
    Generate table schema
    """
    klass = (
        class_or_instance
        if isinstance(class_or_instance, type)
        else type(class_or_instance)
    )
    attrs = attributes(klass)

    schema = {
        "AttributeDefinitions": [a.attribute_def for a in attrs if a.key_type],
        "TableName": table_name or klass.__name__,
        "KeySchema": [a.key_schema for a in attrs if a.key_type],
    }

    # Set the correct previsioned details
    if provisioned_throughput is None:
        schema["BillingMode"] = BillingMode.PayPerRequest.value
    else:
        schema["BillingMode"] = BillingMode.Provisioned.value
        read_cap, write_cap = provisioned_throughput
        schema["ProvisionedThroughput"] = {
            "ReadCapacityUnits": read_cap,
            "WriteCapacityUnits": write_cap,
        }

    return schema


def asitem(obj) -> Dict[str, Any]:
    """
    Convert a dataclass into a DynamoDB item.
    """
    return {a.name: a.to_dynamo(getattr(obj, a.name)) for a in attributes(obj)}


async def fromitem(klass: Type, data: Dict[str, Any]):
    """
    Convert DynamoDB item into a dataclass.
    """
    values = {
        a.attr_name: await a.from_dynamo(data.get(a.name)) for a in attributes(klass)
    }
    return klass(**values)
