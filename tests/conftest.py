import hashlib
import uuid
from typing import Any

import pytest
from fastapi_users import schemas
from moto import mock_aws
from pydantic import UUID4


class User(schemas.BaseUser):
    """A base class representing an `User`."""

    first_name: str | None


class UserCreate(schemas.BaseUserCreate):
    """A base class representing the creation of an `User`."""

    first_name: str | None


class UserUpdate(schemas.BaseUserUpdate):
    """A base class representing the update of an `User`."""

    pass


class UserOAuth(User, schemas.BaseOAuthAccountMixin):
    """A base class representing an `User` with linked `OAuth` accounts."""

    pass


@pytest.fixture(scope="session", autouse=True)
def global_moto_mock():
    """
    Start Moto DynamoDB mock before any test runs,
    and stop it after all tests are done.
    """
    m = mock_aws()
    m.start()
    yield
    m.stop()


@pytest.fixture
def oauth_account1() -> dict[str, Any]:
    """Return a fake `OAuth` account for testing.

    Returns:
        dict[str, Any]: A `dict` object representing the `OAuth` account.
    """
    return {
        "oauth_name": "service1",
        "access_token": "TOKEN",
        "expires_at": 1579000751,
        "account_id": "user_oauth1",
        "account_email": "king.arthur@camelot.bt",
    }


@pytest.fixture
def oauth_account2() -> dict[str, Any]:
    """Return a fake `OAuth` account for testing.

    Returns:
        dict[str, Any]: A `dict` object representing the `OAuth` account.
    """
    return {
        "oauth_name": "service2",
        "access_token": "TOKEN",
        "expires_at": 1579000751,
        "account_id": "user_oauth2",
        "account_email": "king.arthur@camelot.bt",
    }


@pytest.fixture
def user_id() -> UUID4:
    """Return a randomly generated UUIDv4.

    Returns:
        UUID4: The random UUIDv4 user id.
    """
    return uuid.uuid4()


def hash_string(string: str) -> str:
    """A simple function that returns the SHA256 hash of a given string.
    Args:
        string (str): The string to hash.
    Returns:
        str: The SHA256 hash of the string.
    """
    return hashlib.sha256(string.encode("utf-8")).hexdigest()
