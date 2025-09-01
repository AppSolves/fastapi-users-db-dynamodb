from collections.abc import AsyncGenerator

import aioboto3
import pytest
import pytest_asyncio
from moto import mock_aws

from fastapi_users_db_dynamodb import (
    UUID_ID,
    DynamoDBBaseOAuthAccountTableUUID,
    DynamoDBBaseUserTableUUID,
    DynamoDBUserDatabase,
)
from tests.conftest import DATABASE_REGION


class Base:
    pass


class User(DynamoDBBaseUserTableUUID, Base):
    first_name: str | None


class OAuthBase:
    pass


class OAuthAccount(DynamoDBBaseOAuthAccountTableUUID, OAuthBase):
    pass


class UserOAuth(DynamoDBBaseUserTableUUID, OAuthBase):
    first_name: str | None
    oauth_accounts: list[OAuthAccount]


@pytest_asyncio.fixture
async def dynamodb_user_db() -> AsyncGenerator[DynamoDBUserDatabase, None]:
    with mock_aws():
        session = aioboto3.Session()
        table_name = "users_test"

        db = DynamoDBUserDatabase(
            session,
            DynamoDBBaseUserTableUUID,
            table_name,
            dynamodb_resource_region=DATABASE_REGION,
        )
        yield db


@pytest_asyncio.fixture
async def dynamodb_user_db_oauth() -> AsyncGenerator[DynamoDBUserDatabase, None]:
    with mock_aws():
        session = aioboto3.Session()
        user_table_name = "users_test_oauth"
        oauth_table_name = "oauth_accounts_test"

        db = DynamoDBUserDatabase(
            session,
            UserOAuth,
            user_table_name,
            OAuthAccount,
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
    with pytest.raises(NotImplementedError):
        oauth_account = OAuthAccount()
        await dynamodb_user_db.update_oauth_account(user, oauth_account, {})


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
):
    # Test OAuth accounts
    oauth_account1 = {
        "oauth_name": "service1",
        "access_token": "TOKEN",
        "expires_at": 1579000751,
        "account_id": "user_oauth1",
        "account_email": "king.arthur@camelot.bt",
    }
    oauth_account2 = {
        "oauth_name": "service2",
        "access_token": "TOKEN",
        "expires_at": 1579000751,
        "account_id": "user_oauth2",
        "account_email": "king.arthur@camelot.bt",
    }

    user_create = {"email": "lancelot@camelot.bt", "hashed_password": "guinevere"}

    # Create user
    user = await dynamodb_user_db_oauth.create(user_create)
    assert user.id is not None

    # Add OAuth accounts
    user = await dynamodb_user_db_oauth.add_oauth_account(user, oauth_account1)
    user = await dynamodb_user_db_oauth.add_oauth_account(user, oauth_account2)

    assert len(user.oauth_accounts) == 2
    assert user.oauth_accounts[0].account_id == oauth_account1["account_id"]
    assert user.oauth_accounts[1].account_id == oauth_account2["account_id"]

    # Update OAuth account
    user = await dynamodb_user_db_oauth.update_oauth_account(
        user, user.oauth_accounts[0], {"access_token": "NEW_TOKEN"}
    )
    assert user.oauth_accounts[0].access_token == "NEW_TOKEN"

    # Get by id
    id_user = await dynamodb_user_db_oauth.get(user.id)
    assert id_user is not None
    assert id_user.id == user.id
    assert id_user.oauth_accounts[0].access_token == "NEW_TOKEN"

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
