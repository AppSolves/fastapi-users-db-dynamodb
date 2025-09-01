from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

import aioboto3
import pytest
import pytest_asyncio
from moto import mock_aws
from pydantic import UUID4, BaseModel

from fastapi_users_db_dynamodb import DynamoDBBaseUserTableUUID, DynamoDBUserDatabase
from fastapi_users_db_dynamodb._aioboto3_patch import *  # noqa: F403
from fastapi_users_db_dynamodb.access_token import (
    DynamoDBAccessTokenDatabase,
    DynamoDBBaseAccessTokenTableUUID,
)
from tests.conftest import DATABASE_REGION
from tests.tables import ensure_table_exists


class Base(BaseModel):
    pass


class AccessToken(DynamoDBBaseAccessTokenTableUUID, Base):
    pass


class User(DynamoDBBaseUserTableUUID, Base):
    pass


@pytest_asyncio.fixture
async def dynamodb_access_token_db(
    user_id: UUID4,
) -> AsyncGenerator[DynamoDBAccessTokenDatabase[AccessToken]]:
    with mock_aws():
        session = aioboto3.Session()
        user_table_name = "users_test"
        token_table_name = "access_tokens_test"
        await ensure_table_exists(session, user_table_name, DATABASE_REGION)
        await ensure_table_exists(session, token_table_name, DATABASE_REGION)

        user_db = DynamoDBUserDatabase(
            session,
            DynamoDBBaseUserTableUUID,
            user_table_name,
            dynamodb_resource_region=DATABASE_REGION,
        )
        user = await user_db.create(
            {
                "id": user_id,
                "email": "lancelot@camelot.bt",
                "hashed_password": "guinevere",
            }
        )

        token_db = DynamoDBAccessTokenDatabase(
            session,
            AccessToken,
            token_table_name,
            dynamodb_resource_region=DATABASE_REGION,
        )

        # Vorherigen Token löschen, falls er existiert
        token_obj = await token_db.get_by_token("TOKEN")
        if token_obj:
            await token_db.delete(token_obj)

        yield token_db

        token_obj = await token_db.get_by_token("TOKEN")
        if token_obj:
            await token_db.delete(token_obj)

        await user_db.delete(user)


@pytest.mark.asyncio
async def test_queries(
    dynamodb_access_token_db: DynamoDBAccessTokenDatabase[AccessToken],
    user_id: UUID4,
):
    access_token_create = {"token": "TOKEN", "user_id": user_id}

    access_token = await dynamodb_access_token_db.create(access_token_create)
    assert access_token.token == "TOKEN"
    assert access_token.user_id == user_id

    new_time = datetime.now(timezone.utc)
    updated_access_token = await dynamodb_access_token_db.update(
        access_token, {"created_at": new_time}
    )
    assert updated_access_token.created_at.replace(microsecond=0) == new_time.replace(
        microsecond=0
    )

    token_obj = await dynamodb_access_token_db.get_by_token(access_token.token)
    assert token_obj is not None

    token_obj = await dynamodb_access_token_db.get_by_token(
        access_token.token, max_age=datetime.now(timezone.utc) + timedelta(hours=1)
    )
    assert token_obj is None

    token_obj = await dynamodb_access_token_db.get_by_token(
        access_token.token, max_age=datetime.now(timezone.utc) - timedelta(hours=1)
    )
    assert token_obj is not None

    token_obj = await dynamodb_access_token_db.get_by_token("NOT_EXISTING_TOKEN")
    assert token_obj is None

    await dynamodb_access_token_db.delete(access_token)
    deleted_token = await dynamodb_access_token_db.get_by_token(access_token.token)
    assert deleted_token is None


@pytest.mark.asyncio
async def test_insert_existing_token(
    dynamodb_access_token_db: DynamoDBAccessTokenDatabase[AccessToken],
    user_id: UUID4,
):
    access_token_create = {"token": "TOKEN", "user_id": user_id}

    await dynamodb_access_token_db.create(access_token_create)

    with pytest.raises(Exception):
        await dynamodb_access_token_db.create(access_token_create)
