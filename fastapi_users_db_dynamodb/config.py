from enum import StrEnum
from typing import Any, Literal, TypedDict

__version__: str = "1.0.0"


# Right now, only ON-DEMAND mode is supported!
class BillingMode(StrEnum):
    PAY_PER_REQUEST = "PAY_PER_REQUEST"
    # PROVISIONED = "PROVISIONED"

    def __str__(self) -> str:
        return self.value


class __ConfigMap(TypedDict):
    DATABASE_REGION: str
    # DATABASE_BILLING_MODE: BillingMode
    DATABASE_BILLING_MODE: Literal[BillingMode.PAY_PER_REQUEST]
    DATABASE_USERTABLE_NAME: str
    DATABASE_OAUTHTABLE_NAME: str
    DATABASE_TOKENTABLE_NAME: str


def __create_config():
    __config_map: __ConfigMap = {
        "DATABASE_REGION": "eu-central-1",
        "DATABASE_BILLING_MODE": BillingMode.PAY_PER_REQUEST,
        "DATABASE_USERTABLE_NAME": "user",
        "DATABASE_OAUTHTABLE_NAME": "oauth_account",
        "DATABASE_TOKENTABLE_NAME": "accesstoken",
    }

    def get(key: str, default: Any = None) -> Any:
        return __config_map.get(key, default)

    def set(key: str, value: Any) -> None:
        if key not in __config_map:
            raise KeyError(f"Unknown config key: {key}")
        expected_type = type(__config_map[key])
        if not isinstance(value, expected_type):
            raise TypeError(
                f"Invalid type for '{key}'. Expected {expected_type.__name__}, got {type(value).__name__}."
            )
        __config_map[key] = value

    return get, set


get, set = __create_config()
