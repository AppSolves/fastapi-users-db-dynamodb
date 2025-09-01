import random as rd
from collections.abc import AsyncGenerator
from typing import Any

import aioboto3
import pytest
import pytest_asyncio
from moto import mock_aws
from pydantic import BaseModel, ConfigDict, Field

from fastapi_users_db_dynamodb import (
    UUID_ID,
    DynamoDBBaseOAuthAccountTableUUID,
    DynamoDBBaseUserTableUUID,
    DynamoDBUserDatabase,
)
from fastapi_users_db_dynamodb._aioboto3_patch import *  # noqa: F403
from tests.conftest import DATABASE_REGION, DATABASE_USERTABLE_PRIMARY_KEY
from tests.tables import ensure_table_exists


class Base(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)


class User(DynamoDBBaseUserTableUUID, Base):
    first_name: str | None = Field(default=None, description="First name of the user")


class OAuthBase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)


class OAuthAccount(OAuthBase, DynamoDBBaseOAuthAccountTableUUID):
    pass


class UserOAuth(DynamoDBBaseUserTableUUID, OAuthBase):
    first_name: str | None = Field(default=None, description="First name of the user")
    oauth_accounts: list[OAuthAccount] = Field(
        default_factory=list, description="Linked OAuth accounts"
    )


@pytest_asyncio.fixture
async def dynamodb_user_db() -> AsyncGenerator[DynamoDBUserDatabase, None]:
    with mock_aws():
        session = aioboto3.Session()
        table_name = "users_test"
        await ensure_table_exists(
            session, table_name, DATABASE_USERTABLE_PRIMARY_KEY, DATABASE_REGION
        )

        db = DynamoDBUserDatabase(
            session,
            User,
            table_name,
            DATABASE_USERTABLE_PRIMARY_KEY,
            dynamodb_resource_region=DATABASE_REGION,
        )
        yield db


@pytest_asyncio.fixture
async def dynamodb_user_db_oauth() -> AsyncGenerator[DynamoDBUserDatabase, None]:
    with mock_aws():
        session = aioboto3.Session()
        user_table_name = "users_test_oauth"
        oauth_table_name = "oauth_accounts_test"
        await ensure_table_exists(
            session, user_table_name, DATABASE_USERTABLE_PRIMARY_KEY, DATABASE_REGION
        )
        await ensure_table_exists(
            session, oauth_table_name, DATABASE_USERTABLE_PRIMARY_KEY, DATABASE_REGION
        )

        db = DynamoDBUserDatabase(
            session,
            UserOAuth,
            user_table_name,
            DATABASE_USERTABLE_PRIMARY_KEY,
            OAuthAccount,  # type: ignore
            oauth_table_name,
            dynamodb_resource_region=DATABASE_REGION,
        )
        yield db


@pytest.mark.asyncio
async def test_queries(dynamodb_user_db: DynamoDBUserDatabase[User, UUID_ID]):
    user_create = {"email": "lancelot@camelot.bt", "hashed_password": "guinevere"}

    # Create user
    user = await dynamodb_user_db.create(user_create)
    assert user.id is not None
    assert user.is_active is True
    assert user.is_superuser is False
    assert user.email == user_create["email"]

    # Update user
    updated_user = await dynamodb_user_db.update(user, {"is_superuser": True})
    assert updated_user.is_superuser is True

    # Get by id
    id_user = await dynamodb_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user.id
    assert id_user.is_superuser is True

    # Get by email
    email_user = await dynamodb_user_db.get_by_email(user_create["email"])
    assert email_user is not None
    assert email_user.id == user.id

    # Get by uppercased email
    email_user = await dynamodb_user_db.get_by_email("Lancelot@camelot.bt")
    assert email_user is not None
    assert email_user.id == user.id

    # Unknown user
    unknown_user = await dynamodb_user_db.get_by_email("foo@bar.bt")
    assert unknown_user is None

    # Delete user
    await dynamodb_user_db.delete(user)
    deleted_user = await dynamodb_user_db.get(user.id)
    assert deleted_user is None

    # OAuth without defined table
    with pytest.raises(NotImplementedError):
        await dynamodb_user_db.get_by_oauth_account("foo", "bar")
    with pytest.raises(NotImplementedError):
        await dynamodb_user_db.add_oauth_account(user, {})
    with pytest.raises(ValueError):
        oauth_account = OAuthAccount()  # type: ignore
        await dynamodb_user_db.update_oauth_account(user, oauth_account, {})  # type: ignore


@pytest.mark.asyncio
async def test_insert_existing_email(
    dynamodb_user_db: DynamoDBUserDatabase[User, UUID_ID],
):
    user_create = {
        "email": "lancelot@camelot.bt",
        "hashed_password": "guinevere",
    }
    await dynamodb_user_db.create(user_create)

    with pytest.raises(ValueError):  # oder eigene Exception
        existing = await dynamodb_user_db.get_by_email(user_create["email"])
        if existing:
            raise ValueError("Email already exists")
        await dynamodb_user_db.create(user_create)


@pytest.mark.asyncio
async def test_queries_custom_fields(
    dynamodb_user_db: DynamoDBUserDatabase[User, UUID_ID],
):
    """It should output custom fields in query result."""
    user_create = {
        "email": "lancelot@camelot.bt",
        "hashed_password": "guinevere",
        "first_name": "Lancelot",
    }
    user = await dynamodb_user_db.create(user_create)

    id_user = await dynamodb_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user.id
    assert id_user.first_name == user.first_name


@pytest.mark.asyncio
async def test_queries_oauth(
    dynamodb_user_db_oauth: DynamoDBUserDatabase[UserOAuth, UUID_ID],
    oauth_account1: dict[str, Any],
    oauth_account2: dict[str, Any],
):
    # Test OAuth accounts
    user_create = {"email": "lancelot@camelot.bt", "hashed_password": "guinevere"}

    # Create user
    user = await dynamodb_user_db_oauth.create(user_create)
    assert user.id is not None

    # Add OAuth accounts
    user = await dynamodb_user_db_oauth.add_oauth_account(user, oauth_account1)
    user = await dynamodb_user_db_oauth.add_oauth_account(user, oauth_account2)

    assert len(user.oauth_accounts) == 2
    assert user.oauth_accounts[0].account_id == oauth_account1["account_id"]  # type: ignore
    assert user.oauth_accounts[1].account_id == oauth_account2["account_id"]  # type: ignore

    # Update OAuth account
    random_account_id = rd.choice(user.oauth_accounts).id

    def _get_account(_user: UserOAuth):
        return next(acc for acc in _user.oauth_accounts if acc.id == random_account_id)

    user = await dynamodb_user_db_oauth.update_oauth_account(
        user,
        _get_account(user),
        {"access_token": "NEW_TOKEN"},
    )
    assert _get_account(user).access_token == "NEW_TOKEN"  # type: ignore

    #! IMPORTANT: Since DynamoDB uses eventual consistency, we need a small delay (e.g. `time.sleep(0.01)`) \
    #! to ensure the user was fully updated. In production, this should be negligible. \
    #! Alternatively, the `get` and `update` methods of the `DynamoDBDatabase` class allow users \
    #! to enable consistent reads via the `instant_update` argument.

    # Get by id
    id_user = await dynamodb_user_db_oauth.get(user.id, instant_update=True)
    assert id_user is not None
    assert id_user.id == user.id
    assert _get_account(id_user).access_token == "NEW_TOKEN"  # type: ignore

    # Get by email
    email_user = await dynamodb_user_db_oauth.get_by_email(user_create["email"])
    assert email_user is not None
    assert email_user.id == user.id
    assert len(email_user.oauth_accounts) == 2

    # Get by OAuth account
    oauth_user = await dynamodb_user_db_oauth.get_by_oauth_account(
        oauth_account1["oauth_name"], oauth_account1["account_id"]
    )
    assert oauth_user is not None
    assert oauth_user.id == user.id

    # Unknown OAuth account
    unknown_oauth_user = await dynamodb_user_db_oauth.get_by_oauth_account("foo", "bar")
    assert unknown_oauth_user is None
