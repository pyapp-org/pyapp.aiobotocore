from dataclasses import MISSING, Field, dataclass, asdict
from typing import List, Callable, Any


def attribute(
    *,
    validators: List[Callable[[Any], None]] = None,
    default=MISSING,
    default_factory=MISSING,
    init=True,
    repr=True,
    hash=None,
    compare=True,
    metadata=None
):
    """
    A wrapper around `dataclass.field` to extend the `metadata` argument with
    DynamoDB specific information.
    """
    if default is not MISSING and default_factory is not MISSING:
        raise ValueError("cannot specify both default and default_factory")
    metadata = metadata or {}
    metadata["__dynamodb__"] = {"validators": validators}
    return Field(default, default_factory, init, repr, hash, compare, metadata)


if __name__ == "__main__":
    from pprint import pprint

    def main():
        @dataclass
        class Record:
            id: str
            age: int = attribute(default=10)
            items: List[str] = attribute(default_factory=None)

        record = Record(id="123-456789-0987", age=5)

        d = asdict(record)
        pprint(d)

    main()
