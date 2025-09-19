from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
import pytest_asyncio
from aiopynamodb.models import Model
from pydantic import UUID4

from fastapi_users_db_dynamodb import (
    DynamoDBBaseUserTableUUID,
    DynamoDBUserDatabase,
    config,
)
from fastapi_users_db_dynamodb._generics import classproperty, now_utc
from fastapi_users_db_dynamodb.access_token import (
    DynamoDBAccessTokenDatabase,
    DynamoDBBaseAccessTokenTableUUID,
)


class Base(Model):
    """A base class representing a PynamoDB `Model`.

    Args:
        Model (_type_): The PynamoDB base class definition.
    """

    pass


class AccessToken(DynamoDBBaseAccessTokenTableUUID, Base):
    """A class representing an `AccessToken` object.

    Args:
        DynamoDBBaseAccessTokenTableUUID (_type_): The underlying table object.
        Base (_type_): The PynamoDB base class definition.
    """

    @classproperty
    def __tablename__(self) -> str:
        return config.get("DATABASE_TOKENTABLE_NAME") + "_test"

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
            return config.get("DATABASE_TOKENTABLE_NAME") + "_test"

        @classproperty
        def region(self) -> str:
            return config.get("DATABASE_REGION")

        @classproperty
        def billing_mode(self) -> str:
            return config.get("DATABASE_BILLING_MODE").value


class User(DynamoDBBaseUserTableUUID, Base):
    """A class representing a `User` obejct.

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


@pytest_asyncio.fixture
async def dynamodb_access_token_db(
    user_id: UUID4,
) -> AsyncGenerator[DynamoDBAccessTokenDatabase[AccessToken]]:
    """Create a new `AccessToken` Database and yield it to other tests.

    Args:
        user_id (UUID4): The user id for the default user.

    Returns:
        AsyncGenerator[DynamoDBAccessTokenDatabase[AccessToken]]: The `AccessToken` Database instance.

    Yields:
        Iterator[AsyncGenerator[DynamoDBAccessTokenDatabase[AccessToken]]]: The `AccessToken` Database instance.
    """
    user_db = DynamoDBUserDatabase(User)
    user = await user_db.create(
        User(
            id=user_id,
            email="lancelot@camelot.bt",
            hashed_password="guinevere",
        )
    )

    token_db = DynamoDBAccessTokenDatabase(AccessToken)

    yield token_db

    await user_db.delete(user)


@pytest.mark.asyncio
async def test_queries(
    dynamodb_access_token_db: DynamoDBAccessTokenDatabase[AccessToken],
    user_id: UUID4,
):
    """Test default queries to the `AccessToken` Database.

    Args:
        dynamodb_access_token_db (DynamoDBAccessTokenDatabase[AccessToken]): The database instance to use.
        user_id (UUID4): The default user id to use.
    """
    access_token_create = {"token": "TOKEN", "user_id": user_id}

    # Create
    access_token = await dynamodb_access_token_db.create(access_token_create)
    assert access_token.token == "TOKEN"
    assert access_token.user_id == user_id
    with pytest.raises(
        ValueError,
        match="AccessToken must implement and store value 'user_id'.",
    ):
        access_token_create.pop("user_id")
        await dynamodb_access_token_db.create(access_token_create)

    # Update
    new_time = now_utc()
    updated_access_token = await dynamodb_access_token_db.update(
        access_token, {"created_at": new_time}
    )
    assert updated_access_token.created_at.replace(microsecond=0) == new_time.replace(
        microsecond=0
    )

    # Get
    token_obj = await dynamodb_access_token_db.get_by_token(access_token.token)
    assert token_obj is not None

    token_obj = await dynamodb_access_token_db.get_by_token(
        access_token.token, max_age=now_utc() + timedelta(hours=1)
    )
    assert token_obj is None

    token_obj = await dynamodb_access_token_db.get_by_token(
        access_token.token, max_age=now_utc() - timedelta(hours=1)
    )
    assert token_obj is not None

    token_obj = await dynamodb_access_token_db.get_by_token("NOT_EXISTING_TOKEN")
    assert token_obj is None

    # Create existing
    with pytest.raises(
        ValueError,
        match="Access token could not be created because it already exists.",
    ):
        token = AccessToken()
        token.token = "TOKEN"
        token.user_id = user_id
        await dynamodb_access_token_db.create(token)

    # Delete
    await dynamodb_access_token_db.delete(access_token)
    with pytest.raises(ValueError, match="Access token could not be deleted"):
        await dynamodb_access_token_db.delete(access_token)

    deleted_token = await dynamodb_access_token_db.get_by_token(access_token.token)
    assert deleted_token is None

    # Update non-existent
    new_time = now_utc()
    with pytest.raises(
        ValueError,
        match="Access token could not be updated because it does not exist.",
    ):
        await dynamodb_access_token_db.update(access_token, {"created_at": new_time})


@pytest.mark.asyncio
async def test_insert_existing_token(
    dynamodb_access_token_db: DynamoDBAccessTokenDatabase[AccessToken],
    user_id: UUID4,
):
    """Test function that creates and saves an already existing `AccessToken`.

    Args:
        dynamodb_access_token_db (DynamoDBAccessTokenDatabase[AccessToken]): The database instance to use.
        user_id (UUID4): The default user id to use.
    """
    access_token_create = {"token": "TOKEN", "user_id": user_id}

    token = await dynamodb_access_token_db.get_by_token(access_token_create["token"])
    if token:
        await dynamodb_access_token_db.delete(token)

    await dynamodb_access_token_db.create(access_token_create)

    with pytest.raises(ValueError):
        await dynamodb_access_token_db.create(access_token_create)
