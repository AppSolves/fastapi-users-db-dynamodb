import uuid
from typing import Any

import pytest
from fastapi_users import schemas
from moto import mock_aws
from pydantic import UUID4


class User(schemas.BaseUser):
    first_name: str | None


class UserCreate(schemas.BaseUserCreate):
    first_name: str | None


class UserUpdate(schemas.BaseUserUpdate):
    pass


class UserOAuth(User, schemas.BaseOAuthAccountMixin):
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
    return {
        "oauth_name": "service1",
        "access_token": "TOKEN",
        "expires_at": 1579000751,
        "account_id": "user_oauth1",
        "account_email": "king.arthur@camelot.bt",
    }


@pytest.fixture
def oauth_account2() -> dict[str, Any]:
    return {
        "oauth_name": "service2",
        "access_token": "TOKEN",
        "expires_at": 1579000751,
        "account_id": "user_oauth2",
        "account_email": "king.arthur@camelot.bt",
    }


@pytest.fixture
def user_id() -> UUID4:
    return uuid.uuid4()
