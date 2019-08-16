from enum import Enum

__all__ = (
    "NoDefault",
    "DataType",
    "IndexDataTypes",
    "KeyType",
    "BillingMode",
    "ReturnValues",
)


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
    Binary = "B"
    Bool = "BOOL"
    List = "L"
    Map = "M"
    NumberSet = "NS"
    StringSet = "SS"
    BinarySet = "BS"


IndexDataTypes = (DataType.Number, DataType.String, DataType.Binary)


class KeyType(str, Enum):
    """
    Key types supported by DynamoDB
    """

    Hash = "HASH"
    Range = "RANGE"


class BillingMode(str, Enum):
    """
    Billing mode
    """

    Provisioned = "PROVISIONED"
    PayPerRequest = "PAY_PER_REQUEST"


class ReturnValues(str, Enum):
    """
    Return values
    """

    ReturnNone = "NONE"
    AllOld = "ALL_OLD"


class Select(str, Enum):
    """
    Attributes to select
    """

    All = "All_ATTRIBUTES"
    AllProjected = "ALL_PROJECTED_ATTRIBUTES"
    Count = "COUNT"
    Specific = "SPECIFIC_ATTRIBUTES"


class Operation(Enum):
    """
    Comparisons supported by expressions
    """

    EQ = "{} == {}"
    GT = "{} > {}"
    GE = "{} >= {}"
    LT = "{} < {}"
    LE = "{} <= {}"
    STARTSWITH = "{}.startswith({})"
    BETWEEN = "{}.between({}, {})"
