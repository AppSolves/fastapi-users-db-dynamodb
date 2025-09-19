"""FastAPI Users database adapter for AWS DynamoDB.

This adapter mirrors the SQLAlchemy adapter's public API and return types as closely
as reasonably possible while using DynamoDB via `aiopynamodb`.

Usage notes:
- This adapter is expected to function correctly, but it is still advisable to exercise
  caution in production environments (yet).
- The Database will create non existent tables by default. You can customize the configuration
  inside `config.py` using the `get` and `set` methods.
- For now, tables will require ON-DEMAND mode, since traffic is unpredictable in all auth tables!
"""

import uuid
from typing import TYPE_CHECKING, Any, Generic

from aiopynamodb.attributes import BooleanAttribute, NumberAttribute, UnicodeAttribute
from aiopynamodb.exceptions import DeleteError, PutError
from aiopynamodb.indexes import AllProjection, GlobalSecondaryIndex
from aiopynamodb.models import Model
from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import ID, OAP, UP

from . import config
from ._generics import UUID_ID, classproperty
from .attributes import GUID, TransformingUnicodeAttribute
from .config import __version__  # noqa: F401
from .tables import ensure_tables_exist


class DynamoDBBaseUserTable(Model, Generic[ID]):
    """Base user table schema for DynamoDB."""

    @classproperty
    def __tablename__(self) -> str:
        return config.get("DATABASE_USERTABLE_NAME")

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
            return config.get("DATABASE_USERTABLE_NAME")

        @classproperty
        def region(self) -> str:
            return config.get("DATABASE_REGION")

        @classproperty
        def billing_mode(self) -> str:
            return config.get("DATABASE_BILLING_MODE").value

    class EmailIndex(GlobalSecondaryIndex):
        """Enable the `email` attribute to be a Global Secondary Index.

        Args:
            GlobalSecondaryIndex (_type_): The Global Secondary Index base class.
        """

        class Meta:
            """The metadata for the Global Secondary Index."""

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
    """A base class representing `User` objects with unique IDs.

    Args:
        DynamoDBBaseUserTable (_type_): The underlying table object.
    """

    if TYPE_CHECKING:  # pragma: no cover
        id: UUID_ID
    else:
        id: GUID = GUID(hash_key=True, default=uuid.uuid4)


class DynamoDBBaseOAuthAccountTable(Model, Generic[ID]):
    """Base OAuth account table schema for DynamoDB."""

    @classproperty
    def __tablename__(self) -> str:
        return config.get("DATABASE_OAUTHTABLE_NAME")

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
            return config.get("DATABASE_OAUTHTABLE_NAME")

        @classproperty
        def region(self) -> str:
            return config.get("DATABASE_REGION")

        @classproperty
        def billing_mode(self) -> str:
            return config.get("DATABASE_BILLING_MODE").value

    class AccountIdIndex(GlobalSecondaryIndex):
        """Enable the `account_id` attribute to be a Global Secondary Index.

        Args:
            GlobalSecondaryIndex (_type_): The Global Secondary Index base class.
        """

        class Meta:
            """The metadata for the Global Secondary Index."""

            index_name: str = "account_id-index"
            projection = AllProjection()

        account_id = UnicodeAttribute(hash_key=True)

    class OAuthNameIndex(GlobalSecondaryIndex):
        """Enable the `oauth_name` attribute to be a Global Secondary Index.

        Args:
            GlobalSecondaryIndex (_type_): The Global Secondary Index base class.
        """

        class Meta:
            """The metadata for the Global Secondary Index."""

            index_name: str = "oauth_name-index"
            projection = AllProjection()

        oauth_name = UnicodeAttribute(hash_key=True)

    if TYPE_CHECKING:  # pragma: no cover
        id: ID
        oauth_name: str
        access_token: str
        expires_at: int | None
        refresh_token: str | None
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
    """A base class representing `OAuthAccount` objects with unique IDs.

    Args:
        DynamoDBBaseOAuthAccountTable (_type_): The underlying table object.
    """

    # MANDATORY GSI (MUST BE IMPLEMENTED)
    class UserIdIndex(GlobalSecondaryIndex):
        """Enable the `user_id` attribute to be a Global Secondary Index.

        Args:
            GlobalSecondaryIndex (_type_): The Global Secondary Index base class.
        """

        class Meta:
            """The metadata for the Global Secondary Index."""

            index_name = "user_id-index"
            projection = AllProjection()

        user_id = GUID(hash_key=True)

    if TYPE_CHECKING:  # pragma: no cover
        id: UUID_ID
        user_id: UUID_ID
    else:
        id: GUID = GUID(hash_key=True, default=uuid.uuid4)
        user_id: GUID = GUID(null=False)

    # Global Secondary Index
    user_id_index = UserIdIndex()


class DynamoDBUserDatabase(Generic[UP, ID], BaseUserDatabase[UP, ID]):
    """
    Database adapter for AWS DynamoDB using `aiopynamodb`. \
    
    Stores `User` and `OAuth` accounts.
    """

    user_table: type[UP]
    oauth_account_table: type[DynamoDBBaseOAuthAccountTable] | None

    def __init__(
        self,
        user_table: type[UP],
        oauth_account_table: type[DynamoDBBaseOAuthAccountTable] | None = None,
    ):
        """Initialize the database adapter.

        Args:
            user_table (type[UP]): The underlying table for storing `User` accounts.
            oauth_account_table (type[DynamoDBBaseOAuthAccountTable] | None, optional): The underlying table for storing `OAuth` accounts. Defaults to None.
        """
        self.user_table = user_table
        self.oauth_account_table = oauth_account_table

    async def _hydrate_oauth_accounts(
        self,
        user: UP,
        instant_update: bool = False,
    ) -> UP:
        """Populate the `oauth_accounts` list of a user by querying the `OAuth` table. \
        
        This mimics *SQLAlchemy*'s lazy relationship loading.

        Args:
            user (UP): The `User` object that should be refreshed.
            instant_update (bool, optional): Whether to use consistent reads. Defaults to False.

        Returns:
            UP: The refreshed user.
        """
        if self.oauth_account_table is None:
            return user
        await ensure_tables_exist(self.oauth_account_table)

        user.oauth_accounts = []  # type: ignore

        if not hasattr(self.oauth_account_table, "user_id_index") or not isinstance(
            self.oauth_account_table.user_id_index,  # type: ignore
            GlobalSecondaryIndex,
        ):
            raise ValueError(
                "Attribute 'user_id_index' not found: OAuthAccount table scheme must implement a Global Secondary Index for attribute 'user_id'."
            )
        async for oauth_acc in self.oauth_account_table.user_id_index.query(  # type: ignore
            user.id,
            consistent_read=instant_update,
        ):
            user.oauth_accounts.append(oauth_acc)  # type: ignore

        return user

    async def get(self, id: ID, instant_update: bool = False) -> UP | None:
        """Get a user by id and hydrate `oauth_accounts` if available.

        Args:
            id (ID): The id of the user account.
            instant_update (bool, optional): Whether to use consistent reads. Defaults to False.

        Returns:
            UP | None: The `User` object, if found.
        """
        await ensure_tables_exist(self.user_table)  # type: ignore

        try:
            user = await self.user_table.get(id, consistent_read=instant_update)  # type: ignore
            user = await self._hydrate_oauth_accounts(user, instant_update)
            return user
        except self.user_table.DoesNotExist:  # type: ignore
            return None

    async def get_by_email(self, email: str, instant_update: bool = False) -> UP | None:
        """Get a user by email using the email **Global Secondary Index** (case-insensitive).

        Args:
            email (str): The email of the user account.
            instant_update (bool, optional): Whether to use consistent reads. Defaults to False.

        Returns:
            UP | None: The `User` object, if found.
        """
        await ensure_tables_exist(self.user_table)  # type: ignore

        email_lower = email.lower()
        async for user in self.user_table.email_index.query(  # type: ignore
            email_lower,
            consistent_read=instant_update,
            limit=1,
        ):
            user = await self._hydrate_oauth_accounts(user, instant_update)
            return user
        return None

    async def get_by_oauth_account(
        self,
        oauth: str,
        account_id: str,
        instant_update: bool = False,
    ) -> UP | None:
        """Find a user by oauth provider and `account_id`.

        Args:
            oauth (str): The name of the `OAuth` provider.
            account_id (str): The id of the `OAuth` account.
            instant_update (bool, optional): Whether to use consistent reads. Defaults to False.

        Raises:
            NotImplementedError: If the `OAuth` table was not specified upon initialization.

        Returns:
            UP | None: The `User` object, if found.
        """
        if self.oauth_account_table is None:
            raise NotImplementedError()
        await ensure_tables_exist(self.user_table, self.oauth_account_table)  # type: ignore

        async for oauth_acc in self.oauth_account_table.account_id_index.query(
            account_id,
            consistent_read=instant_update,
            filter_condition=self.oauth_account_table.oauth_name == oauth,  # type: ignore
            limit=1,
        ):
            try:
                user = await self.user_table.get(  # type: ignore
                    oauth_acc.user_id,
                    consistent_read=instant_update,
                )
                user = await self._hydrate_oauth_accounts(user, instant_update)
                return user
            except self.user_table.DoesNotExist:  # type: ignore # pragma: no cover
                return None
        return None

    async def create(self, create_dict: dict[str, Any] | UP) -> UP:
        """Create a new user and return an instance of UP.

        Args:
            create_dict (dict[str, Any] | UP): A dictionary holding the data of the user account.

        Raises:
            ValueError: If the user account could not be created for whatever reason.
            ValueError: If the user account could not be created because the table did not exist.

        Returns:
            UP: The newly created `User` object.
        """
        await ensure_tables_exist(self.user_table)  # type: ignore

        if isinstance(create_dict, dict):
            user = self.user_table(**create_dict)
        else:
            user = create_dict
        try:
            await user.save(  # type: ignore
                condition=self.user_table.id.does_not_exist()
                & self.user_table.email.does_not_exist()  # type: ignore
            )
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "User account could not be created because it already exists."
                ) from e
            raise ValueError(  # pragma: no cover
                "User account could not be created because the table does not exist."
            ) from e
        return user

    async def update(self, user: UP, update_dict: dict[str, Any]) -> UP:
        """Update a user with `update_dict` and return the updated UP instance.

        Args:
            user (UP): The `User` instance to be updated.
            update_dict (dict[str, Any]): A dictionary with the changes that should be applied.

        Raises:
            ValueError: If the user account could not be updated for whatever reason.
            ValueError: If the user account could not be updated because the table did not exist.

        Returns:
            UP: The refreshed `User` object.
        """
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
            raise ValueError(  # pragma: no cover
                "User account could not be updated because the table does not exist."
            ) from e

    async def delete(self, user: UP) -> None:
        """Delete a user.

        Args:
            user (UP): The `User` object to be deleted.

        Raises:
            ValueError: If the user account could not be deleted for whatever reason.
            ValueError: If the user account could not be deleted because the table did not exist.
        """
        await ensure_tables_exist(self.user_table)  # type: ignore

        try:
            await user.delete(condition=self.user_table.id.exists())  # type: ignore
        except DeleteError as e:
            raise ValueError("User account could not be deleted.") from e
        except PutError as e:  # pragma: no cover
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "User account could not be deleted because it does not exist."
                ) from e

    async def add_oauth_account(self, user: UP, create_dict: dict[str, Any]) -> UP:
        """Add an `OAuth` account and return the refreshed user (UP).

        Args:
            user (UP): The `User` object, which the newly created `OAuth` account should be linked to.
            create_dict (dict[str, Any]): A dictionary holding the data of the `OAuth` account.

        Raises:
            NotImplementedError: If the `OAuth` table was not specified upon initialization.
            ValueError: If the `OAuth` account could not be created for whatever reason.
            ValueError: If the `OAuth` account could not be created because the table did not exist.

        Returns:
            UP: The refreshed `User` object.
        """
        if self.oauth_account_table is None:
            raise NotImplementedError()
        await ensure_tables_exist(self.user_table, self.oauth_account_table)  # type: ignore

        try:
            create_dict["user_id"] = getattr(create_dict, "user_id", user.id)
            oauth_account = self.oauth_account_table(**create_dict)
            await oauth_account.save(
                condition=self.oauth_account_table.id.does_not_exist()  # type: ignore
                & self.oauth_account_table.account_id.does_not_exist()  # type: ignore
            )
            user.oauth_accounts.append(oauth_account)  # type: ignore
        except PutError as e:  # pragma: no cover
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "OAuth account could not be added because it already exists."
                ) from e
            raise ValueError(
                "OAuth account could not be added because the table does not exist."
            ) from e

        return user

    async def update_oauth_account(
        self,
        user: UP,
        oauth_account: OAP,  # type: ignore
        update_dict: dict[str, Any],
    ) -> UP:
        """Update an OAuth account and return the refreshed user (UP).

        Args:
            user (UP): The `User` object, which the updated `OAuth` account should be linked to.
            oauth_account (OAP): The existing `OAuth` account to be updated.
            update_dict (dict[str, Any]): A dictionary with the changes that should be applied.

        Raises:
            NotImplementedError: If the `OAuth` table was not specified upon initialization.
            ValueError: If the `OAuth` account could not be updated for whatever reason.
            ValueError: If the `OAuth` account could not be updated because the table did not exist.

        Returns:
            UP: The refreshed `User` object.
        """
        if self.oauth_account_table is None:
            raise NotImplementedError()
        await ensure_tables_exist(self.user_table, self.oauth_account_table)  # type: ignore

        try:
            for k, v in update_dict.items():
                setattr(oauth_account, k, v)
            await oauth_account.save(  # type: ignore
                condition=self.oauth_account_table.id.exists()  # type: ignore
                & self.oauth_account_table.account_id.exists()  # type: ignore
            )
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "OAuth account could not be updated because it does not exist."
                ) from e
            raise ValueError(  # pragma: no cover
                "OAuth account could not be updated because the table does not exist."
            ) from e

        for acc in user.oauth_accounts:  # type: ignore
            if acc.id == oauth_account.id:
                for k, v in update_dict.items():
                    setattr(acc, k, v)
                break

        return user
