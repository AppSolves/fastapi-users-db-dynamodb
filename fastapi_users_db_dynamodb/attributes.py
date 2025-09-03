from collections.abc import Callable

from aiopynamodb.attributes import Attribute, UnicodeAttribute
from aiopynamodb.constants import STRING

from ._generics import UUID_ID


class GUID(Attribute[UUID_ID]):
    """
    Custom PynamoDB attribute to store UUIDs as strings.
    Ensures value is always a UUID object in Python.
    """

    attr_type = STRING
    python_type = UUID_ID

    def serialize(self, value):
        if value is None:
            return None
        if isinstance(value, UUID_ID):
            return str(value)
        return str(UUID_ID(value))

    def deserialize(self, value):
        if value is None:
            return None
        if not isinstance(value, UUID_ID):
            return UUID_ID(value)
        return value


class TransformingUnicodeAttribute(UnicodeAttribute):
    """
    A UnicodeAttribute that automatically transforms its value.

    Example: lowercasing, uppercasing, capitalizing.
    """

    def __init__(self, transform: Callable[[str], str] | None = None, **kwargs):
        """
        :param transform: A callable to transform the string (e.g., str.lower, str.upper)
        :param kwargs: Other UnicodeAttribute kwargs
        """
        super().__init__(**kwargs)
        self.transform = transform

    def serialize(self, value):
        if value is not None and self.transform:
            value = self.transform(value)
        return super().serialize(value)

    def deserialize(self, value):
        value = super().deserialize(value)
        if value is not None and self.transform:
            value = self.transform(value)
        return value
