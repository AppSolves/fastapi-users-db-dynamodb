"""FastAPI Users database adapter for AWS DynamoDB.

This adapter mirrors the SQLAlchemy adapter's public API and return types as closely
as reasonably possible while using DynamoDB via aioboto3.

Usage notes:
- You can pass a long-lived aioboto3 resource (created once during app startup)
  via the `dynamodb_resource` parameter to avoid creating a resource on every call:
      async with aioboto3.Session().resource("dynamodb", region_name=...) as resource:
          adapter = DynamoDBUserDatabase(
              session, user_table, "users", oauth_account_table, "oauth_accounts",
              dynamodb_resource=resource
          )
  If you don't provide `dynamodb_resource`, this adapter will create a short-lived
  resource per operation (safe, but less optimal).
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic, get_type_hints

import aioboto3
from boto3.dynamodb.conditions import Attr
from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import ID, OAP, UP
from pydantic import BaseModel, ConfigDict, Field

from fastapi_users_db_dynamodb._aioboto3_patch import *  # noqa: F403
from fastapi_users_db_dynamodb.generics import GUID

__version__ = "1.0.0"

UUID_ID = uuid.UUID


class DynamoDBBaseUserTable(BaseModel, Generic[ID]):
    """Base user table schema for DynamoDB."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    __tablename__ = "user"

    if TYPE_CHECKING:
        id: ID
    email: str = Field(..., description="The email of the user")
    hashed_password: str = Field(..., description="The hashed password of the user")
    is_active: bool = Field(
        default=True, description="Whether the user is marked as active in the database"
    )
    is_superuser: bool = Field(
        default=False, description="Whether the user has admin rights"
    )
    is_verified: bool = Field(
        default=False, description="Whether the user has verified their email"
    )


class DynamoDBBaseUserTableUUID(DynamoDBBaseUserTable[UUID_ID]):
    id: UUID_ID = Field(default_factory=uuid.uuid4, description="The ID for the user")


class DynamoDBBaseOAuthAccountTable(Generic[ID]):
    """Base OAuth account table schema for DynamoDB."""

    __tablename__ = "oauth_account"

    if TYPE_CHECKING:
        id: ID
    oauth_name: str = Field(..., description="The name of the OAuth social provider")
    access_token: str = Field(
        ..., description="The access token linked with the OAuth account"
    )
    expires_at: int | None = Field(
        default=None, description="The timestamp at which this account expires"
    )
    refresh_token: str | None = Field(
        default=None, description="The refresh token associated with this OAuth account"
    )
    account_id: str = Field(..., description="The ID of this OAuth account")
    account_email: str = Field(
        ..., description="The email associated with this OAuth account"
    )


class DynamoDBBaseOAuthAccountTableUUID(DynamoDBBaseUserTable[UUID_ID]):
    id: UUID_ID = Field(
        default_factory=uuid.uuid4, description="The ID for the OAuth account"
    )
    user_id: GUID = Field(..., description="The user ID this OAuth account belongs to")


class DynamoDBUserDatabase(Generic[UP, ID], BaseUserDatabase[UP, ID]):
    """
    Database adapter for AWS DynamoDB using aioboto3.

    :param session: aioboto3.Session instance (not an actual DynamoDB resource).
    :param user_table: Python class used to construct returned user objects (callable).
    :param user_table_name: DynamoDB table name for users.
    :param oauth_account_table: Optional class to construct oauth-account objects.
    :param oauth_table_name: Optional DynamoDB table name for oauth accounts.
    :param dynamodb_resource: Optional aioboto3 resource object (async context manager result)
                              created with `async with session.resource("dynamodb") as r:`. If
                              provided, the adapter will reuse it (recommended).
    """

    session: aioboto3.Session
    user_table: type[UP]
    oauth_account_table: type[DynamoDBBaseOAuthAccountTable] | None
    user_table_name: str
    oauth_account_table_name: str | None
    _resource: Any | None
    _resource_region: str | None

    def __init__(
        self,
        session: aioboto3.Session,
        user_table: type[UP],
        user_table_name: str,
        oauth_account_table: type[DynamoDBBaseOAuthAccountTable] | None = None,
        oauth_account_table_name: str | None = None,
        dynamodb_resource: Any | None = None,
        dynamodb_resource_region: str | None = None,
    ):
        self.session = session
        self.user_table = user_table
        self.oauth_account_table = oauth_account_table
        self.user_table_name = user_table_name
        self.oauth_account_table_name = oauth_account_table_name

        self._resource = dynamodb_resource
        self._resource_region = dynamodb_resource_region

    @asynccontextmanager
    async def _table(self, table_name: str, region: str | None = None):
        """Async context manager that yields a Table object.

        If a long-lived resource was provided at init, it's reused (no enter/exit).
        Otherwise a short-lived resource is created and cleaned up per call.
        """
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

    def _ensure_id_str(self, value: Any) -> str:
        """Normalize id to string for DynamoDB keys."""
        return str(value)

    def _extract_id_from_user(self, user_obj: Any) -> str:
        """Extract the `id` from a user object/dict/ORM/Pydantic model."""

        if isinstance(user_obj, dict):
            idv = user_obj.get("id")

        elif hasattr(user_obj, "dict") and callable(getattr(user_obj, "dict")):
            try:
                idv = user_obj.dict().get("id")
            except Exception:
                idv = getattr(user_obj, "id", None)

        elif hasattr(user_obj, "id"):
            idv = getattr(user_obj, "id", None)

        elif hasattr(user_obj, "__dict__"):
            idv = vars(user_obj).get("id")
        else:
            raise ValueError("Cannot extract 'id' from provided user object")
        if idv is None:
            raise ValueError("User object has no 'id' field")
        return self._ensure_id_str(idv)

    def _item_to_user(self, item: dict[str, Any] | None) -> UP | None:
        """Convert a DynamoDB item (dict) to an instance of user_table (UP)."""
        if item is None:
            return None

        try:
            hints = get_type_hints(self.user_table)
            if (
                "id" in hints
                and hints["id"] is uuid.UUID
                and isinstance(item.get("id"), str)
            ):
                item = {**item, "id": uuid.UUID(item["id"])}
        except Exception:
            pass

        return self.user_table(**item)

    def _ensure_email_lower(self, data: dict[str, Any]) -> None:
        """Lower-case email in-place if present."""
        if "email" in data and isinstance(data["email"], str):
            data["email"] = data["email"].lower()

    async def get(self, id: ID | str) -> UP | None:
        """Get a user by id."""
        id_str = self._ensure_id_str(id)
        async with self._table(self.user_table_name, self._resource_region) as table:
            resp = await table.get_item(Key={"id": id_str})
            item = resp.get("Item")
            return self._item_to_user(item)

    async def get_by_email(self, email: str) -> UP | None:
        """Get a user by email (case-insensitive: emails are stored lowercased)."""
        email_norm = email.lower()
        async with self._table(self.user_table_name, self._resource_region) as table:
            resp = await table.scan(
                FilterExpression=Attr("email").eq(email_norm),
                Limit=1,
            )
            items = resp.get("Items", [])
            if not items:
                return None
            return self._item_to_user(items[0])

    async def get_by_oauth_account(self, oauth: str, account_id: str) -> UP | None:
        """Find a user by oauth provider and provider account id."""
        if self.oauth_account_table is None or self.oauth_account_table_name is None:
            raise NotImplementedError()

        async with self._table(
            self.oauth_account_table_name, self._resource_region
        ) as oauth_table:
            resp = await oauth_table.scan(
                FilterExpression=Attr("oauth_name").eq(oauth)
                & Attr("account_id").eq(account_id),
                Limit=1,
            )
            items = resp.get("Items", [])
            if not items:
                return None

            user_id = items[0].get("user_id")
            if user_id is None:
                return None

            return await self.get(user_id)

    async def create(self, create_dict: dict[str, Any]) -> UP:
        """Create a new user and return an instance of UP."""
        item = dict(create_dict)
        if "id" not in item or item["id"] is None:
            item["id"] = str(uuid.uuid4())
        else:
            item["id"] = self._ensure_id_str(item["id"])

        self._ensure_email_lower(item)

        async with self._table(self.user_table_name, self._resource_region) as table:
            await table.put_item(Item=item)

            resp = await table.get_item(Key={"id": item["id"]})
            stored = resp.get("Item", item)

        refreshed_user = self._item_to_user(stored)
        if refreshed_user is None:
            raise ValueError("Could not cast DB item to User model")
        return refreshed_user

    async def update(self, user: UP, update_dict: dict[str, Any]) -> UP:
        """Update a user with update_dict and return the updated UP instance."""
        user_id = self._extract_id_from_user(user)
        async with self._table(self.user_table_name, self._resource_region) as table:
            resp = await table.get_item(Key={"id": user_id})
            current = resp.get("Item", {})
            if not current:
                raise ValueError("User not found")

            merged = {**current, **update_dict}

            self._ensure_email_lower(merged)

            await table.put_item(Item=merged)

            refreshed = (await table.get_item(Key={"id": user_id})).get("Item", merged)

        refreshed_user = self._item_to_user(refreshed)
        if refreshed_user is None:
            raise ValueError("Could not cast DB item to User model")
        return refreshed_user

    async def delete(self, user: UP) -> None:
        """Delete a user."""
        user_id = self._extract_id_from_user(user)
        async with self._table(self.user_table_name, self._resource_region) as table:
            await table.delete_item(Key={"id": user_id})

    async def add_oauth_account(self, user: UP, create_dict: dict[str, Any]) -> UP:
        """Add an OAuth account for `user`. Returns the refreshed user (UP)."""
        if self.oauth_account_table is None or self.oauth_account_table_name is None:
            raise NotImplementedError()

        oauth_item = dict(create_dict)
        if "id" not in oauth_item or oauth_item["id"] is None:
            oauth_item["id"] = str(uuid.uuid4())

        user_id = self._extract_id_from_user(user)
        oauth_item["user_id"] = user_id

        async with self._table(
            self.oauth_account_table_name, self._resource_region
        ) as oauth_table:
            await oauth_table.put_item(Item=oauth_item)

        refreshed_user = await self.get(user_id)
        if refreshed_user is None:
            raise ValueError("Refreshed user is None")

        try:
            oauth_obj = (
                self.oauth_account_table(**oauth_item)
                if self.oauth_account_table is not None
                else oauth_item
            )
            if hasattr(refreshed_user, "oauth_accounts"):
                getattr(refreshed_user, "oauth_accounts").append(oauth_obj)
        except Exception:
            pass

        return refreshed_user

    async def update_oauth_account(
        self,
        user: UP,
        oauth_account: OAP,  # type: ignore
        update_dict: dict[str, Any],
    ) -> UP:
        """Update an OAuth account and return the refreshed user (UP)."""
        if self.oauth_account_table is None or self.oauth_account_table_name is None:
            raise NotImplementedError()

        oauth_id = None
        if isinstance(oauth_account, dict):
            oauth_id = oauth_account.get("id")
        elif hasattr(oauth_account, "dict") and callable(
            getattr(oauth_account, "dict")
        ):
            try:
                oauth_id = oauth_account.dict().get("id")  # type: ignore
            except Exception:
                oauth_id = getattr(oauth_account, "id", None)
        elif hasattr(oauth_account, "id"):
            oauth_id = getattr(oauth_account, "id", None)
        elif hasattr(oauth_account, "__dict__"):
            oauth_id = vars(oauth_account).get("id")

        if oauth_id is None:
            raise ValueError("oauth_account has no 'id' field")

        oauth_id_str = self._ensure_id_str(oauth_id)

        async with self._table(
            self.oauth_account_table_name, self._resource_region
        ) as oauth_table:
            resp = await oauth_table.get_item(Key={"id": oauth_id_str})
            current = resp.get("Item", {})
            if not current:
                raise ValueError("OAuth account not found")

            merged = {**current, **update_dict}
            await oauth_table.put_item(Item=merged)

        user_id = self._extract_id_from_user(user)
        refreshed_user = await self.get(user_id)
        if refreshed_user is None:
            raise ValueError("Could not cast DB item to User model")
        return refreshed_user
