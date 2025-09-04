from collections.abc import Callable

from aiopynamodb.attributes import Attribute, UnicodeAttribute
from aiopynamodb.constants import STRING

from ._generics import UUID_ID


class GUID(Attribute[UUID_ID]):
    """
    Custom PynamoDB `Attribute` to store `UUID`s as strings. \
    
    Ensures value is always a `UUID` object in Python.
    """

    attr_type = STRING
    python_type = UUID_ID

    def serialize(self, value):
        """Serialize a value for later storage in DynamoDB.

        Args:
            value (UUID_ID): The `UUID` to be serialized.

        Returns:
            str: The serialized `UUID` as a string.
        """
        if value is None:
            return None
        if isinstance(value, UUID_ID):
            return str(value)
        return str(UUID_ID(value))

    def deserialize(self, value):
        """Deserialize a value from DynamoDB and convert it back into a python object.

        Args:
            value (str): The serialized string representation of the `UUID`.

        Returns:
            UUID_ID: The deserialized `UUID` python object.
        """
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
        """Initialize the `Attribute` class.

        Args:
            transform (Callable[[str], str] | None, optional): A callable to transform the string (e.g., `str.lower`, `str.upper`). Defaults to None.
        """
        super().__init__(**kwargs)
        self.transform = transform

    def serialize(self, value):
        """Serialize a value for later storage in DynamoDB.

        Args:
            value (str): The string to be transformed and serialized.

        Returns:
            str: The transformed and serialized object.
        """
        if value is not None and self.transform:
            value = self.transform(value)
        return super().serialize(value)

    def deserialize(self, value):
        """Deserialize a value from DynamoDB and convert it back into a python object.

        Args:
            value (str): The serialized string representation of the object.

        Returns:
            str: The deserialized python object.
        """
        value = super().deserialize(value)
        if value is not None and self.transform:
            value = self.transform(value)
        return value
