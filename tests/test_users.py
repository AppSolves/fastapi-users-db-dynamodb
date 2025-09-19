import random as rd
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from aiopynamodb.attributes import UnicodeAttribute
from aiopynamodb.models import Model

from fastapi_users_db_dynamodb import (
    UUID_ID,
    DynamoDBBaseOAuthAccountTableUUID,
    DynamoDBBaseUserTableUUID,
    DynamoDBUserDatabase,
    classproperty,
    config,
)


class Base(Model):
    """A base class representing a PynamoDB `Model`.

    Args:
        Model (_type_): The PynamoDB base class definition.
    """

    pass


class User(DynamoDBBaseUserTableUUID, Base):
    """A class representing an `User` object.

    Args:
        DynamoDBBaseUserTableUUID (_type_): The underlying table object.
        Base (_type_): The PynamoDB base class definition.
    """

    @classproperty
    def __tablename__(self) -> str:
        return config.get("DATABASE_USERTABLE_NAME") + "_test"

    class Meta:
        """The required `Meta` definitions for PynamoDB.

        Args:
            table_name (str): The name of the table.
            region (str): The AWS region string where the table should be created.
            billing_mode (str): The billing mode to use when creating the table. \
            Currently only supports `PAY_PER_REQUEST`.
        """

        @classproperty
        def table_name(self) -> str:
            return config.get("DATABASE_USERTABLE_NAME") + "_test"

        @classproperty
        def region(self) -> str:
            return config.get("DATABASE_REGION")

        @classproperty
        def billing_mode(self) -> str:
            return config.get("DATABASE_BILLING_MODE").value

    if TYPE_CHECKING:
        first_name: str | None = None
    else:
        first_name = UnicodeAttribute(null=True)


class OAuthBase(Model):
    """The base class representing `OAuth` related models.

    Args:
        Model (_type_): The PynamoDB base class definition.
    """

    pass


class OAuthAccount(DynamoDBBaseOAuthAccountTableUUID, OAuthBase):
    """A class representing an `OAuthAccount` object.

    Args:
        DynamoDBBaseOAuthAccountTableUUID (_type_): The underlying table object.
        OAuthBase (_type_): The base class for `OAuth`.
    """

    pass


class UserOAuth(DynamoDBBaseUserTableUUID, OAuthBase):
    """A class representing an `UserOAuth` object.

    Args:
        DynamoDBBaseUserTableUUID (_type_): The underlying table object.
        OAuthBase (_type_): The base class representing `OAuth` related models.
    """

    @classproperty
    def __tablename__(self) -> str:
        return config.get("DATABASE_OAUTHTABLE_NAME") + "_test"

    class Meta:
        """The required `Meta` definitions for PynamoDB.

        Args:
            table_name (str): The name of the table.
            region (str): The AWS region string where the table should be created.
            billing_mode (str): The billing mode to use when creating the table. \
            Currently only supports `PAY_PER_REQUEST`.
        """

        @classproperty
        def table_name(self) -> str:
            return config.get("DATABASE_OAUTHTABLE_NAME") + "_test"

        @classproperty
        def region(self) -> str:
            return config.get("DATABASE_REGION")

        @classproperty
        def billing_mode(self) -> str:
            return config.get("DATABASE_BILLING_MODE").value

    if TYPE_CHECKING:
        first_name: str | None = None
    else:
        first_name = UnicodeAttribute(null=True)

    oauth_accounts: list[OAuthAccount] = []


@pytest_asyncio.fixture
async def dynamodb_user_db() -> AsyncGenerator[DynamoDBUserDatabase, None]:
    """Create and yield a new `User` database instance for other tests to use.

    Returns:
        AsyncGenerator[DynamoDBUserDatabase, None]: The `User` database instance.

    Yields:
        Iterator[AsyncGenerator[DynamoDBUserDatabase, None]]: The `User` database instance.
    """
    db = DynamoDBUserDatabase(User)
    yield db


@pytest_asyncio.fixture
async def dynamodb_user_db_oauth() -> AsyncGenerator[DynamoDBUserDatabase, None]:
    """Create and yield a new `OAuth` database instance for other tests to use.

    Returns:
        AsyncGenerator[DynamoDBUserDatabase, None]: The `OAuth` database instance.

    Yields:
        Iterator[AsyncGenerator[DynamoDBUserDatabase, None]]: The `OAuth` database instance.
    """
    db = DynamoDBUserDatabase(UserOAuth, OAuthAccount)
    yield db


@pytest.mark.asyncio
async def test_queries(dynamodb_user_db: DynamoDBUserDatabase[User, UUID_ID]):
    """Test basic **CRUD** operations on a `User`.

    Args:
        dynamodb_user_db (DynamoDBUserDatabase[User, UUID_ID]): The `User` database instance to use.
    """
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
    with pytest.raises(
        ValueError,
        match="User account could not be updated because it does not exist.",
    ):
        fake_user = User()
        fake_user.email = "blabla@gmail.com"
        fake_user.hashed_password = "crypticpassword"
        await dynamodb_user_db.update(fake_user, {"is_superuser": True})

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
    with pytest.raises(ValueError, match="User account could not be deleted"):
        await dynamodb_user_db.delete(user)
    deleted_user = await dynamodb_user_db.get(user.id)
    assert deleted_user is None

    # OAuth without defined table
    with pytest.raises(NotImplementedError):
        await dynamodb_user_db.get_by_oauth_account("foo", "bar")
    with pytest.raises(NotImplementedError):
        await dynamodb_user_db.add_oauth_account(user, {})
    with pytest.raises(NotImplementedError):
        oauth_account = OAuthAccount()  # type: ignore
        await dynamodb_user_db.update_oauth_account(user, oauth_account, {})  # type: ignore


@pytest.mark.asyncio
async def test_insert_existing_email(
    dynamodb_user_db: DynamoDBUserDatabase[User, UUID_ID],
):
    """Test inserting an email, which already exists.

    Args:
        dynamodb_user_db (DynamoDBUserDatabase[User, UUID_ID]): The `User` database instance to use.
    """
    user_create = {
        "email": "lancelot@camelot.bt",
        "hashed_password": "guinevere",
    }
    user = await dynamodb_user_db.create(user_create)
    with pytest.raises(
        ValueError,
        match="User account could not be created because it already exists.",
    ):
        user_create["id"] = str(user.id)
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
    """Test basic **CRUD** operations (especially querying) on custom fields.

    Args:
        dynamodb_user_db (DynamoDBUserDatabase[User, UUID_ID]): The `User` database instance to use.
    """
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
    user_id: UUID_ID,
    monkeypatch,
):
    """Test `OAuth` implemenatation and basic **CRUD** operations.

    Args:
        dynamodb_user_db_oauth (DynamoDBUserDatabase[UserOAuth, UUID_ID]): The `OAuth` database instance to use.
        oauth_account1 (dict[str, Any]): A fake `OAuth` account for testing.
        oauth_account2 (dict[str, Any]): Another fake `OAuth` account for testing.
        user_id (UUID_ID): The default user id to use.
    """
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

    #! NOTE: Since DynamoDB uses eventual consistency, we need a small delay (e.g. `time.sleep(0.01)`) \
    #! to ensure the user was fully updated. In production, this should be negligible. \
    #! Alternatively, most methods of the `DynamoDBDatabase` class (e.g. `get`, `update`, ...) allow users \
    #! to enable consistent reads via the `instant_update` argument.

    # Get by id
    id_user = await dynamodb_user_db_oauth.get(user.id)
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

    with pytest.raises(
        ValueError,
        match="OAuth account could not be updated because it does not exist.",
    ):
        user = UserOAuth()
        oauth_account = OAuthAccount()
        oauth_account.user_id = user_id
        oauth_account.oauth_name = "blabla_provider"
        oauth_account.account_id = "blabla_id"
        oauth_account.account_email = "blabla@gmail.com"
        await dynamodb_user_db_oauth.update_oauth_account(
            user,
            oauth_account,
            {"access_token": "NEW_TOKEN"},
        )

    with pytest.raises(
        ValueError,
        match="OAuthAccount table scheme must implement a Global Secondary Index",
    ):
        monkeypatch.setattr(
            dynamodb_user_db_oauth.oauth_account_table,
            "user_id_index",
            None,
        )
        await dynamodb_user_db_oauth._hydrate_oauth_accounts(oauth_user)
