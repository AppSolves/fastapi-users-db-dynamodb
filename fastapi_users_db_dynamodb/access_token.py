"""FastAPI Users access token database adapter for AWS DynamoDB.

This adapter mirrors the SQLAlchemy adapter's public API and return types as closely
as reasonably possible while using DynamoDB via aioboto3.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Generic, get_type_hints

import aioboto3
from botocore.exceptions import ClientError
from fastapi_users.authentication.strategy.db import AP, AccessTokenDatabase
from fastapi_users.models import ID
from pydantic import BaseModel, ConfigDict, Field

from fastapi_users_db_dynamodb._aioboto3_patch import *  # noqa: F403
from fastapi_users_db_dynamodb.generics import UUID_ID

DATABASE_TOKENTABLE_PRIMARY_KEY: str = "token"


class DynamoDBBaseAccessTokenTable(BaseModel, Generic[ID]):
    """Base access token table schema for DynamoDB."""

    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)

    __tablename__ = "accesstoken"

    token: str = Field(..., description="The token value of the AccessToken object")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="The date of creation of the AccessToken object",
    )
    if TYPE_CHECKING:
        user_id: ID


class DynamoDBBaseAccessTokenTableUUID(DynamoDBBaseAccessTokenTable[UUID_ID]):
    user_id: UUID_ID = Field(..., description="The user ID this token belongs to")


class DynamoDBAccessTokenDatabase(Generic[AP], AccessTokenDatabase[AP]):
    """
    Access token database adapter for AWS DynamoDB using aioboto3.

    :param session: aioboto3.Session instance (not an actual DynamoDB resource).
    :param access_token_table: Python class used to construct returned objects (callable).
    :param table_name: DynamoDB table name for access tokens.
    :param dynamodb_resource: Optional aioboto3 resource object (async context manager result).
    """

    session: aioboto3.Session
    access_token_table: type[AP]
    table_name: str
    primary_key: str = DATABASE_TOKENTABLE_PRIMARY_KEY
    _resource: Any | None
    _resource_region: str | None

    def __init__(
        self,
        session: aioboto3.Session,
        access_token_table: type[AP],
        table_name: str,
        primary_key: str = DATABASE_TOKENTABLE_PRIMARY_KEY,
        dynamodb_resource: Any | None = None,
        dynamodb_resource_region: Any | None = None,
    ):
        self.session = session
        self.access_token_table = access_token_table
        self.table_name = table_name
        self.primary_key = primary_key
        self._resource = dynamodb_resource
        self._resource_region = dynamodb_resource_region

    @asynccontextmanager
    async def _table(self, table_name: str, region: str | None = None):
        """Async context manager that yields a Table object."""
        if self._resource is not None:
            table = await self._resource.Table(table_name)
            yield table
        else:
            if region is None:
                raise ValueError(
                    "Parameter `region` must be specified when `dynamodb_resource` is omitted"
                )
            async with self.session.resource(
                "dynamodb", region_name=region
            ) as dynamodb:
                table = await dynamodb.Table(table_name)
                yield table

    def _item_to_access_token(self, item: dict[str, Any] | None) -> AP | None:
        """Convert a DynamoDB item (dict) to an instance of access_token_table (AP)."""
        if item is None:
            return None

        try:
            hints = get_type_hints(self.access_token_table)
            if (
                "user_id" in hints
                and hints["user_id"] is UUID_ID
                and isinstance(item.get("user_id"), str)
            ):
                item = {**item, "user_id": UUID_ID(item["user_id"])}

            if "created_at" in item and isinstance(item["created_at"], str):
                item["created_at"] = datetime.fromisoformat(item["created_at"])
        except Exception:
            pass

        return self.access_token_table(**item)

    def _ensure_token(self, token: Any) -> str:
        """Normalize token to string for DynamoDB keys."""
        return str(token)

    async def get_by_token(
        self, token: str, max_age: datetime | None = None
    ) -> AP | None:
        """Retrieve an access token by token string."""
        async with self._table(self.table_name, self._resource_region) as table:
            resp = await table.get_item(
                Key={self.primary_key: self._ensure_token(token)}
            )
            item = resp.get("Item")

            if item is None:
                return None

            if max_age is not None:
                created_at = datetime.fromisoformat(item["created_at"])
                if created_at < max_age:
                    return None

            return self._item_to_access_token(item)

    async def create(self, create_dict: dict[str, Any]) -> AP:
        """Create a new access token and return an instance of AP."""
        item = dict(create_dict)

        if "token" not in item or item["token"] is None:
            item["token"] = uuid.uuid4().hex[:43]
        if "created_at" not in item or not isinstance(item["created_at"], str):
            item["created_at"] = datetime.now(timezone.utc).isoformat()
        if isinstance(item.get("user_id"), uuid.UUID):
            item["user_id"] = str(item["user_id"])

        async with self._table(self.table_name, self._resource_region) as table:
            try:
                await table.put_item(
                    Item=item,
                    ConditionExpression="attribute_not_exists(#token)",
                    ExpressionAttributeNames={"#token": self.primary_key},
                )
            except ClientError as e:
                if (
                    e.response.get("Error", {}).get("Code")
                    == "ConditionalCheckFailedException"
                ):
                    raise ValueError(f"Token {item['token']} already exists.")
                raise

            access_token = self._item_to_access_token(item)
            if access_token is None:
                raise ValueError("Could not cast DB item to AccessToken model")

        return access_token

    async def update(self, access_token: AP, update_dict: dict[str, Any]) -> AP:
        """Update an existing access token."""

        token_dict: dict = (
            access_token.model_dump()  # type: ignore
            if hasattr(access_token, "model_dump") and callable(access_token.model_dump)  # type: ignore
            else vars(access_token)
            if hasattr(access_token, "__dict__")
            else dict(access_token)
            if isinstance(access_token, dict)
            else vars(access_token)
        )

        token_dict.update(update_dict)

        if isinstance(token_dict.get("user_id"), uuid.UUID):
            token_dict["user_id"] = str(token_dict["user_id"])
        if isinstance(token_dict.get("created_at"), datetime):
            token_dict["created_at"] = token_dict["created_at"].isoformat()

        async with self._table(self.table_name, self._resource_region) as table:
            try:
                await table.put_item(
                    Item=token_dict,
                    ConditionExpression="attribute_exists(#token)",
                    ExpressionAttributeNames={"#token": self.primary_key},
                )
            except ClientError as e:
                if (
                    e.response.get("Error", {}).get("Code")
                    == "ConditionalCheckFailedException"
                ):
                    raise ValueError(f"Token {token_dict['token']} does not exist.")
                raise

        updated = self._item_to_access_token(token_dict)
        if updated is None:
            raise ValueError("Could not cast DB item to AccessToken model")
        return updated

    async def delete(self, access_token: AP) -> None:
        """Delete an access token."""
        token = getattr(access_token, "token", None) or (
            access_token.get("token") if isinstance(access_token, dict) else None
        )
        if token is None:
            raise ValueError("access_token has no 'token' field")

        async with self._table(self.table_name, self._resource_region) as table:
            try:
                await table.delete_item(
                    Key={self.primary_key: self._ensure_token(token)},
                    ConditionExpression="attribute_exists(#token)",
                    ExpressionAttributeNames={"#token": self.primary_key},
                )
            except ClientError as e:
                if (
                    e.response.get("Error", {}).get("Code")
                    == "ConditionalCheckFailedException"
                ):
                    raise ValueError(f"Token {token} does not exist.")
                raise
