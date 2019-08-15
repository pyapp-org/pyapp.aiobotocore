from collections import OrderedDict
from typing import TypeVar, Generic, Type, Optional, Dict, Any, Sequence, Callable, List, Union

from .constants import DataType, KeyType, NoDefault, IndexDataTypes
from .exceptions import ValidationError, InvalidKey, MultipleKeys

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
            raise InvalidKey(f"Only {IndexDataTypes} types can be indexed")

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
        old_value = instance.__dict__.get(self.attr_name)
        if old_value != value:
            instance.__dict__[self.attr_name] = value

            # Track changes
            try:
                instance.__updated__.add(self.attr_name)
            except AttributeError:
                pass

    def __set_name__(self, owner: Union[Type["Table"], Type["Item"]], name: str):
        self.set_attrs_from_name(name)
        owner.__attributes__.append(self)
        owner.__annotations__[name] = self.python_type

        if self.key_type:
            if not hasattr(owner, "__table_keys__"):
                raise InvalidKey("Keys can only be defined on a table.")

            if self.key_type in owner.__table_keys__:
                raise MultipleKeys(f"Multiple {self.key_type.name} keys defined on {owner}")

            owner.__table_keys__[self.key_type] = self

    def set_attrs_from_name(self, attr_name: str):
        """
        Apply the attribute name to Attribute instance
        """
        self.attr_name = attr_name
        self.name = self.name or attr_name

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


def _create_init(attrs: Sequence[Attribute]):
    func_def = [
        f"def __init__(self, {', '.join(f'{a.attr_name} = NoDefault' for a in attrs)}):",
        f"    d = self.__dict__"
    ]
    func_def.extend(f"    if {a.attr_name} is not NoDefault:\n"
                    f"        d['{a.attr_name}'] = {a.attr_name}" for a in attrs)
    func_def.append("    self.__updated__ = set()")
    func_def = "\n".join(func_def)

    local_vars = {}
    exec(func_def, {"NoDefault": NoDefault}, local_vars)
    return local_vars['__init__']


class BaseItem:
    """
    Item base class
    """
    __attributes__: List[Attribute]

    def __new__(cls, *args, **kwargs):
        if cls.__init__ is object.__init__:
            init_func = _create_init(cls.__attributes__)
            cls.__init__ = init_func
        return super().__new__(cls)


class ItemMeta(type):
    """
    Meta class to automate the creation of Table classes.
    """

    @classmethod
    def __prepare__(mcs, class_name, bases, **_):
        return OrderedDict()

    def __new__(mcs, class_name, bases, attrs):
        attrs["__attributes__"] = []
        if "__annotations__" not in attrs:
            attrs["__annotations__"] = {}
        return super().__new__(mcs, class_name, bases, attrs)


class Item(BaseItem, metaclass=ItemMeta):
    """
    Item base class
    """


class TableMeta(ItemMeta):
    """
    Meta class to automate the creation of Table classes.
    """
    def __new__(mcs, class_name, bases, attrs, name: str = None):
        attrs["__tablename__"] = name or attrs.get("__tablename__", class_name)
        attrs["__table_keys__"] = {}
        return super().__new__(mcs, class_name, bases, attrs)


class Table(BaseItem, metaclass=TableMeta):
    """
    Table base class
    """
    __table_name__: str
    __table_keys__: Dict[KeyType, Attribute]
