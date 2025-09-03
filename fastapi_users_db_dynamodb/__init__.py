"""FastAPI Users database adapter for AWS DynamoDB.

This adapter mirrors the SQLAlchemy adapter's public API and return types as closely
as reasonably possible while using DynamoDB via aiopynamodb.

Usage notes:
- This adapter is expected to function correctly, but it is still advisable to exercise
  caution in production environments (yet).
- This will create non existent tables by default. You can tweak the creation inside generics.py
- This will require ON-DEMAND mode, since traffic is unpredictable in all auth tables!
"""

import uuid
from typing import TYPE_CHECKING, Any, Generic, Optional

from aiopynamodb.attributes import BooleanAttribute, NumberAttribute, UnicodeAttribute
from aiopynamodb.exceptions import DeleteError, PutError
from aiopynamodb.indexes import AllProjection, GlobalSecondaryIndex
from aiopynamodb.models import Model
from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import ID, OAP, UP

from . import config
from ._generics import UUID_ID
from .attributes import GUID, TransformingUnicodeAttribute
from .tables import ensure_tables_exist

__version__: str = "1.0.0"


class DynamoDBBaseUserTable(Model, Generic[ID]):
    """Base user table schema for DynamoDB."""

    __tablename__: str = config.get("DATABASE_USERTABLE_NAME")

    class Meta:
        table_name: str = config.get("DATABASE_USERTABLE_NAME")
        region: str = config.get("DATABASE_REGION")
        billing_mode: str = config.get("DATABASE_BILLING_MODE").value

    class EmailIndex(GlobalSecondaryIndex):
        class Meta:
            index_name: str = "email-index"
            projection = AllProjection()

        email = TransformingUnicodeAttribute(transform=str.lower, hash_key=True)

    if TYPE_CHECKING:  # pragma: no cover
        id: ID
        email: str
        hashed_password: str
        is_active: bool
        is_superuser: bool
        is_verified: bool
    else:
        email = TransformingUnicodeAttribute(transform=str.lower, null=False)
        hashed_password = UnicodeAttribute(null=False)
        is_active = BooleanAttribute(default=True, null=False)
        is_superuser = BooleanAttribute(default=False, null=False)
        is_verified = BooleanAttribute(default=False, null=False)

    # Global Secondary Index
    email_index = EmailIndex()


class DynamoDBBaseUserTableUUID(DynamoDBBaseUserTable[UUID_ID]):
    if TYPE_CHECKING:  # pragma: no cover
        id: UUID_ID
    else:
        id: GUID = GUID(hash_key=True, default=uuid.uuid4)


class DynamoDBBaseOAuthAccountTable(Model, Generic[ID]):
    """Base OAuth account table schema for DynamoDB."""

    __tablename__: str = config.get("DATABASE_OAUTHTABLE_NAME")

    class Meta:
        table_name: str = config.get("DATABASE_OAUTHTABLE_NAME")
        region: str = config.get("DATABASE_REGION")
        billing_mode: str = config.get("DATABASE_BILLING_MODE").value

    class AccountIdIndex(GlobalSecondaryIndex):
        class Meta:
            index_name: str = "account_id-index"
            projection = AllProjection()

        account_id = UnicodeAttribute(hash_key=True)

    class OAuthNameIndex(GlobalSecondaryIndex):
        class Meta:
            index_name: str = "oauth_name-index"
            projection = AllProjection()

        oauth_name = UnicodeAttribute(hash_key=True)

    if TYPE_CHECKING:  # pragma: no cover
        id: ID
        oauth_name: str
        access_token: str
        expires_at: Optional[int]
        refresh_token: Optional[str]
        account_id: str
        account_email: str
    else:
        oauth_name = UnicodeAttribute(null=False)
        access_token = UnicodeAttribute(null=False)
        expires_at = NumberAttribute(null=True)
        refresh_token = UnicodeAttribute(null=True)
        account_id = UnicodeAttribute(null=False)
        account_email = TransformingUnicodeAttribute(transform=str.lower, null=False)

    # Global Secondary Index
    account_id_index = AccountIdIndex()
    oauth_name_index = OAuthNameIndex()


class DynamoDBBaseOAuthAccountTableUUID(DynamoDBBaseOAuthAccountTable[UUID_ID]):
    if TYPE_CHECKING:  # pragma: no cover
        id: UUID_ID
        user_id: UUID_ID
    else:
        id: GUID = GUID(hash_key=True, default=uuid.uuid4)
        user_id: GUID = GUID(null=False)


class DynamoDBUserDatabase(Generic[UP, ID], BaseUserDatabase[UP, ID]):
    """
    Database adapter for AWS DynamoDB using aiopynamodb.
    """

    user_table: type[UP]
    oauth_account_table: type[DynamoDBBaseOAuthAccountTable] | None

    def __init__(
        self,
        user_table: type[UP],
        oauth_account_table: type[DynamoDBBaseOAuthAccountTable] | None = None,
    ):
        self.user_table = user_table
        self.oauth_account_table = oauth_account_table

    async def get(self, id: ID, instant_update: bool = False) -> UP | None:
        """Get a user by id and hydrate oauth_accounts if available."""
        await ensure_tables_exist(self.user_table)  # type: ignore

        try:
            return await self.user_table.get(id, consistent_read=instant_update)  # type: ignore
        except self.user_table.DoesNotExist:  # type: ignore
            return None

    async def get_by_email(self, email: str, instant_update: bool = False) -> UP | None:
        """Get a user by email using the email GSI (case-insensitive)."""
        await ensure_tables_exist(self.user_table)  # type: ignore

        email_lower = email.lower()
        async for user in self.user_table.email_index.query(  # type: ignore
            email_lower,
            consistent_read=instant_update,
        ):
            return user
        return None

    async def get_by_oauth_account(
        self,
        oauth: str,
        account_id: str,
        instant_update: bool = False,
    ) -> UP | None:
        """Find a user by oauth provider and account_id."""
        if self.oauth_account_table is None:
            raise NotImplementedError()
        await ensure_tables_exist(self.user_table, self.oauth_account_table)  # type: ignore

        async for oauth_acc in self.oauth_account_table.oauth_name_index.query(
            oauth,
            consistent_read=instant_update,
        ):
            if oauth_acc.account_id == account_id:
                try:
                    return await self.user_table.get(  # type: ignore
                        oauth_acc.user_id,
                        consistent_read=instant_update,
                    )
                except self.user_table.DoesNotExist:  # type: ignore
                    return None
        return None

    async def create(self, create_dict: dict[str, Any] | UP) -> UP:
        """Create a new user and return an instance of UP."""
        await ensure_tables_exist(self.user_table)  # type: ignore

        if isinstance(create_dict, dict):
            user = self.user_table(**create_dict)
        else:
            user = create_dict
        try:
            await user.save(condition=self.user_table.id.does_not_exist())  # type: ignore
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "User account could not be created because it already exists."
                ) from e
            raise ValueError(
                "User account could not be created because the table does not exist."
            ) from e
        return user

    async def update(self, user: UP, update_dict: dict[str, Any]) -> UP:
        """Update a user with update_dict and return the updated UP instance."""
        await ensure_tables_exist(self.user_table)  # type: ignore

        try:
            for k, v in update_dict.items():
                setattr(user, k, v)
            await user.save(condition=self.user_table.id.exists())  # type: ignore
            return user
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "User account could not be updated because it does not exist."
                ) from e
            raise ValueError(
                "User account could not be updated because the table does not exist."
            ) from e

    async def delete(self, user: UP) -> None:
        """Delete a user."""
        await ensure_tables_exist(self.user_table)  # type: ignore

        try:
            await user.delete(condition=self.user_table.id.exists())  # type: ignore
        except DeleteError as e:
            raise ValueError("User account could not be deleted.") from e
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "User account could not be deleted because it does not exist."
                ) from e

    async def add_oauth_account(self, user: UP, create_dict: dict[str, Any]) -> UP:
        """Add an OAuth account and return the refreshed user (UP)."""
        if self.oauth_account_table is None:
            raise NotImplementedError()
        await ensure_tables_exist(self.user_table, self.oauth_account_table)  # type: ignore

        try:
            create_dict["user_id"] = getattr(create_dict, "user_id", user.id)
            oauth_account = self.oauth_account_table(**create_dict)
            await oauth_account.save(condition=self.user_table.id.does_not_exist())
            user.oauth_accounts.append(oauth_account)  # type: ignore
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "OAuth account could not be updated because it already exists."
                ) from e
            raise ValueError(
                "OAuth account could not be updated because the table does not exist."
            ) from e

        return user

    async def update_oauth_account(
        self,
        user: UP,
        oauth_account: OAP,  # type: ignore
        update_dict: dict[str, Any],
    ) -> UP:
        """Update an OAuth account and return the refreshed user (UP)."""
        if self.oauth_account_table is None:
            raise NotImplementedError()
        await ensure_tables_exist(self.user_table, self.oauth_account_table)  # type: ignore

        try:
            for k, v in update_dict.items():
                setattr(oauth_account, k, v)
            await oauth_account.save(condition=self.user_table.id.exists())  # type: ignore
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "OAuth account could not be updated because it does not exist."
                ) from e
            raise ValueError(
                "OAuth account could not be updated because the table does not exist."
            ) from e

        for acc in user.oauth_accounts:  # type: ignore
            if acc.id == oauth_account.id:
                for k, v in update_dict.items():
                    setattr(acc, k, v)
                break

        return user
