"""FastAPI Users access token database adapter for AWS DynamoDB."""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic

from aiopynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from aiopynamodb.exceptions import DeleteError, PutError
from aiopynamodb.indexes import AllProjection, GlobalSecondaryIndex
from aiopynamodb.models import Model
from fastapi_users.authentication.strategy.db import AP, AccessTokenDatabase
from fastapi_users.models import ID

from . import config
from ._generics import UUID_ID, classproperty, now_utc
from .attributes import GUID
from .tables import ensure_tables_exist


class DynamoDBBaseAccessTokenTable(Model, Generic[ID]):
    """Base access token table schema for DynamoDB."""

    @classproperty
    def __tablename__(self) -> str:
        return config.get("DATABASE_TOKENTABLE_NAME")

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
            return config.get("DATABASE_TOKENTABLE_NAME")

        @classproperty
        def region(self) -> str:
            return config.get("DATABASE_REGION")

        @classproperty
        def billing_mode(self) -> str:
            return config.get("DATABASE_BILLING_MODE").value

    class CreatedAtIndex(GlobalSecondaryIndex):
        """Enable the `created_at` attribute to be a Global Secondary Index.

        Args:
            GlobalSecondaryIndex (_type_): The Global Secondary Index base class.
        """

        class Meta:
            """The metadata for the Global Secondary Index."""

            index_name: str = "created_at-index"
            projection = AllProjection()

        created_at = UnicodeAttribute(hash_key=True)

    if TYPE_CHECKING:  # pragma: no cover
        user_id: ID
        token: str
        created_at: datetime
    else:
        token = UnicodeAttribute(hash_key=True)
        created_at = UTCDateTimeAttribute(default=now_utc, null=False)

    # Global Secondary Index
    created_at_index = CreatedAtIndex()


class DynamoDBBaseAccessTokenTableUUID(DynamoDBBaseAccessTokenTable[UUID_ID]):
    """A base class representing `AccessToken` objects with unique IDs.

    Args:
        DynamoDBBaseAccessTokenTable (_type_): The underlying table object.
    """

    # OPTIONAL GSI
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
        user_id: UUID_ID
    else:
        user_id: GUID = GUID(null=False)

    # Global Secondary Index
    user_id_index = UserIdIndex()


class DynamoDBAccessTokenDatabase(Generic[AP], AccessTokenDatabase[AP]):
    """
    Access token database adapter for AWS DynamoDB using `aiopynamodb`. \
    
    Stores `AccessToken`s.
    """

    access_token_table: type[AP]

    def __init__(self, access_token_table: type[AP]):
        """Initialize the Database adapter.

        Args:
            access_token_table (type[AP]): The underlying table for storing `AccessToken`s.
        """
        self.access_token_table = access_token_table

    async def get_by_token(
        self,
        token: str,
        max_age: datetime | None = None,
        instant_update: bool = False,
    ) -> AP | None:
        """Retrieve an access token by it's token string.

        Args:
            token (str): The actual token string to be looked for.
            max_age (datetime | None, optional): The maximum age of an access token. Expired ones will not be returned. Defaults to None.
            instant_update (bool, optional): Whether to use consistent reads. Defaults to False.

        Returns:
            AP | None: The access token, if found.
        """
        await ensure_tables_exist(self.access_token_table)  # type: ignore

        try:
            token_obj = await self.access_token_table.get(  # type: ignore
                token,
                consistent_read=instant_update,
            )

            if max_age is not None:
                if token_obj.created_at < max_age:
                    return None
            return token_obj
        except self.access_token_table.DoesNotExist:  # type: ignore
            return None

    async def create(self, create_dict: dict[str, Any] | AP) -> AP:
        """Create a new access token.

        Args:
            create_dict (dict[str, Any] | AP): A dictionary holding the data of the access token.

        Raises:
            ValueError: If the access token could not be created for whatever reason.
            ValueError: If the access token could not be created because the table did not exist.

        Returns:
            AP: The newly created `AccessToken` object.
        """
        await ensure_tables_exist(self.access_token_table)  # type: ignore

        if isinstance(create_dict, dict):
            token = self.access_token_table(**create_dict)
        else:
            token = create_dict
        if not getattr(token, "user_id", None):
            raise ValueError("AccessToken must implement and store value 'user_id'.")

        try:
            await token.save(condition=self.access_token_table.token.does_not_exist())  # type: ignore
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "Access token could not be created because it already exists."
                ) from e
            raise ValueError(  # pragma: no cover
                "Access token could not be created because the table does not exist."
            ) from e
        return token

    async def update(self, access_token: AP, update_dict: dict[str, Any]) -> AP:
        """Update an existing access token.

        Args:
            access_token (AP): The `AccessToken` object to be updated.
            update_dict (dict[str, Any]): A dictionary with the changes that should be applied.

        Raises:
            ValueError: If the access token could not be updated for whatever reason.
            ValueError: If the access token could not be updated because the table did not exist.

        Returns:
            AP: The refreshed `AccessToken` object.
        """
        await ensure_tables_exist(self.access_token_table)  # type: ignore

        try:
            for k, v in update_dict.items():
                setattr(access_token, k, v)
            await access_token.save(condition=self.access_token_table.token.exists())  # type: ignore
            return access_token
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "Access token could not be updated because it does not exist."
                ) from e
            raise ValueError(  # pragma: no cover
                "Access token could not be updated because the table does not exist."
            ) from e

    async def delete(self, access_token: AP) -> None:
        """Delete an access token.

        Args:
            access_token (AP): The `AccessToken` object to be deleted.

        Raises:
            ValueError: If the access token could not be deleted for whatever reason.
            ValueError: If the access token could not be deleted because the table did not exist.
        """
        await ensure_tables_exist(self.access_token_table)  # type: ignore

        try:
            await access_token.delete(condition=self.access_token_table.token.exists())  # type: ignore
        except DeleteError as e:
            raise ValueError("Access token could not be deleted.") from e
        except PutError as e:  # pragma: no cover
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "Access token could not be deleted because it does not exist."
                ) from e
