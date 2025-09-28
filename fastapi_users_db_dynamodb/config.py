from enum import StrEnum
from typing import Any, Literal, TypedDict

__version__ = "1.0.4"


# Right now, only ON-DEMAND/PAY_PER_REQUEST mode is supported!
class BillingMode(StrEnum):
    """An `Enum` class representing the billing mode of tables. \
    
    **NOTE**: Right now, only `PAY_PER_REQUEST` is supported!

    Args:
        StrEnum (_type_): The base enum class.

    Returns:
        _type_: The enum member, representing the billing mode.
    """

    PAY_PER_REQUEST = "PAY_PER_REQUEST"
    # PROVISIONED = "PROVISIONED"

    def __str__(self) -> str:
        """Return the billing mode directly as a string.

        Returns:
            str: The billing mode's string representation.
        """
        return self.value


class __ConfigMap(TypedDict):
    """A `TypedDict`, enforcing static types on config keys.

    Args:
        TypedDict (_type_): The base type.
    """

    DATABASE_REGION: str
    # DATABASE_BILLING_MODE: BillingMode
    DATABASE_BILLING_MODE: Literal[BillingMode.PAY_PER_REQUEST]
    DATABASE_USERTABLE_NAME: str
    DATABASE_OAUTHTABLE_NAME: str
    DATABASE_TOKENTABLE_NAME: str


def __create_config():
    """Create a new config instance for this python process. \
    Can be modified using this module's `get` and `set` methods.

    Raises:
        KeyError: If the key is not known or not defined.
        TypeError: If the config expected another type for a certain key.

    Returns:
        _type_: The created `get` and `set` methods.
    """
    __config_map: __ConfigMap = {
        "DATABASE_REGION": "eu-central-1",
        "DATABASE_BILLING_MODE": BillingMode.PAY_PER_REQUEST,
        "DATABASE_USERTABLE_NAME": "user",
        "DATABASE_OAUTHTABLE_NAME": "oauth_account",
        "DATABASE_TOKENTABLE_NAME": "accesstoken",
    }

    def get(key: str, default: Any = None) -> Any:
        """Get a value from the config dict.

        Args:
            key (str): The unique key specifying the configuration option.
            default (Any, optional): A default value that is being returned when the key is not found. Defaults to None.

        Returns:
            Any: The configuration value.
        """
        return __config_map.get(key, default)

    def set(key: str, value: Any) -> None:
        """Set/Change a value inside of the config dict.

        Args:
            key (str): The unique key specifying the configuration option.
            value (Any): The new value this configuration option will be changed to.

        Raises:
            KeyError: If the key is not defined or invalid.
            TypeError: If the value's type does not match the options expected type.
        """
        if key not in __config_map:
            raise KeyError(f"Unknown config key: {key}")
        expected_type = type(__config_map[key])  # type: ignore[literal-required]
        if not isinstance(value, expected_type):
            raise TypeError(
                f"Invalid type for '{key}'. Expected {expected_type.__name__}, got {type(value).__name__}."
            )
        __config_map[key] = value  # type: ignore[literal-required]

    return get, set


get, set = __create_config()
