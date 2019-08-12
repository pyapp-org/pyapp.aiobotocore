from typing import Sequence

from .base import Attribute
from .exceptions import NotATable, ValidationError


def get_attributes(obj) -> Sequence[Attribute]:
    """
    Get attributes from a Table (or Table like) instance.
    """
    try:
        return getattr(obj, "__attributes__")
    except AttributeError:
        raise NotATable(f"Object {type(obj)!r} does not have attributes.") from None


def clean(obj):
    """
    Clean a Table (or Table like) instance
    """
    attributes = get_attributes(obj)

    errors = {}

    for attribute in attributes:
        value = attribute.get_attr(obj)
        try:
            value = attribute.clean(value)
        except ValidationError as ex:
            errors[attribute.attr_name] = ex.error_messages
        else:
            attribute.set_attr(obj, value)

    if errors:
        raise ValidationError(errors)
