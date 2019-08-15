from typing import Sequence, Dict

from .base import Attribute
from .constants import KeyType
from .exceptions import NotATable, ValidationError


def get_attributes(obj) -> Sequence[Attribute]:
    """
    Get attributes from a Table (or Table like) instance.
    """
    try:
        return getattr(obj, "__attributes__")
    except AttributeError:
        raise NotATable(f"Object {type(obj)!r} does not have attributes.") from None


def key_attributes(obj) -> Dict[KeyType, Attribute]:
    """
    Get key attributes from a Table (or Table like) instance.
    """
    try:
        return getattr(obj, "__table_keys__")
    except AttributeError:
        raise NotATable(f"Object {type(obj)!r} does not have keys.") from None


async def clean(obj) -> None:
    """
    Clean a Table (or Table like) instance
    """
    attributes = get_attributes(obj)

    errors = {}

    for attribute in attributes:
        value = attribute.get_attr(obj)
        try:
            value = await attribute.clean(value)
        except ValidationError as ex:
            errors[attribute.attr_name] = ex.error_messages
        else:
            attribute.set_attr(obj, value)

    if errors:
        raise ValidationError(errors)


def to_dynamo_key(obj, *, key_type: KeyType = KeyType.Hash) -> Dict[str, dict]:
    """
    Map a Table (of Table like) instances into a DynamoDB Key structure.
    """
    keys = key_attributes(obj)
    attribute = keys[key_type]
    value = attribute.get_attr(obj)
    return attribute.to_dynamo(value)


def to_dynamo(obj, *, updates_only: bool = False) -> Dict[str, dict]:
    """
    Map a Table (of Table like) instance into a DynamoDB Attributes structure.
    """
    attributes = get_attributes(obj)
    if updates_only:
        updates = getattr(obj, "__updated__")
        attributes = [a for a in attributes if a.attr_name in updates]

    document = {}

    for attribute in attributes:
        value = attribute.get_attr(obj)
        document[attribute.name] = attribute.to_dynamo(value)

    return document
