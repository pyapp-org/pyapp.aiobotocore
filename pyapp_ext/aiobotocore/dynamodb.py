import uuid

from datetime import datetime
from typing import Tuple, Callable, Generic, Sequence, Set, Any, _GenericAlias

BasicType = Tuple[str, Callable, Callable]

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
