from collections import OrderedDict
from enum import Enum
from typing import TypeVar, Generic, Type, Optional, Dict, Any, Sequence, Callable

from .exceptions import ValidationError


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
        null: bool = False,
        key_type: KeyType = None,
        attr_name: str = None,
        default: VT_ = NoDefault,
        default_factory: Callable[[], VT_] = NoDefault,
        validators: Sequence[Callable[[VT_], None]] = None,
    ):
        self.name = name
        self.null = null
        self.key_type = key_type
        self.attr_name = attr_name
        self.validators = validators or []

        # Ensure only data types that can be indexed are used if
        # an this attribute is a key field.
        if key_type and self.dynamo_type not in IndexDataTypes:
            raise ValueError(f"Only {IndexDataTypes} types can be indexed")

        # Determine correct default
        if default is NoDefault and default_factory is NoDefault:
            self.default_factory = None
        elif default is NoDefault:
            self.default_factory = default_factory
        elif default_factory is NoDefault:
            self.default_factory = lambda: default
        else:
            raise ValueError("cannot specify both default and default_factory")

    def __get__(self, instance, owner):
        if instance:
            try:
                return instance.__dict__[self.attr_name]
            except KeyError:
                if self.default_factory:
                    return self.default_factory()
        else:
            return self

    def __set__(self, instance, value):
        instance.__dict__[self.attr_name] = value

    def __set_name__(self, owner: Type["Table"], name: str):
        self.set_attrs_from_name(name)
        owner.__attributes__.append(self)

    def set_attrs_from_name(self, attr_name: str):
        """
        Apply the attribute name to Attribute instance
        """
        self.attr_name = attr_name
        self.name = self.name or attr_name
    #
    # def add_to_table(self, attr_name: str, klass: "Table"):
    #     """
    #     Called by the table meta class to apply name to object
    #     """
    #     self.set_attrs_from_name(attr_name)
    #     klass.__attributes__.append(self)

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

    async def clean_value(self, value: Any) -> Optional[VT_]:
        """
        Clean actual value.
        """

    def validate(self, value: Optional[VT_]):
        """
        Execute all validators
        """
        if not self.null and value is None:
            raise ValidationError("A value is required.")

    async def run_validators(self, value: Optional[VT_]):
        """
        Run validators and collect results
        """
        errors = []
        for validator in self.validators:
            try:
                await validator(value)
            except ValidationError as ex:
                errors.extend(ex.messages)
        if errors:
            raise ValidationError(errors)

    async def clean(self, value: Any) -> Optional[VT_]:
        """
        Convert the value's type and run validation. Validation errors
        from to_python and validate are propagated. The correct value is
        returned if no error is raised.
        """
        value = await self.clean_value(value)
        self.validate(value)
        await self.run_validators(value)
        return value

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

    def from_dynamo(self, item: Dict[str, Any]):
        """
        Convert an item back into a value from DynamoDB
        """
        if "NULL" in item:
            return

        try:
            value = item[self.dynamo_type.value]
        except KeyError:
            raise RuntimeWarning()
        else:
            return self.clean_value(value)

    def get_attr(self, obj) -> Any:
        """
        Get attribute from an object
        """
        return getattr(obj, self.attr_name)

    def set_attr(self, obj, value: VT_):
        """
        Set attribute on an object
        """
        setattr(obj, self.attr_name, value)


class TableMeta(type):
    """
    Meta class to automate the creation of Table classes.
    """

    @classmethod
    def __prepare__(mcs, name, bases):
        return OrderedDict()

    def __new__(mcs, class_name, bases, attrs, name: str = None):
        super_new = super().__new__
        attrs["__attributes__"] = []
        attrs["__tablename__"] = name or attrs.get("__tablename__", class_name)
        return super_new(mcs, class_name, bases, attrs)


class Table(metaclass=TableMeta):
    """
    Table base class
    """
