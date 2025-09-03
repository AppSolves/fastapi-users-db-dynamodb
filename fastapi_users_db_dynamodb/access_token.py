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
from ._generics import UUID_ID, now_utc
from .attributes import GUID
from .tables import ensure_tables_exist


class DynamoDBBaseAccessTokenTable(Model, Generic[ID]):
    """Base access token table schema for DynamoDB."""

    __tablename__: str = config.get("DATABASE_TOKENTABLE_NAME")

    class Meta:
        table_name: str = config.get("DATABASE_TOKENTABLE_NAME")
        region: str = config.get("DATABASE_REGION")
        billing_mode: str = config.get("DATABASE_BILLING_MODE").value

    class CreatedAtIndex(GlobalSecondaryIndex):
        class Meta:
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
    if TYPE_CHECKING:  # pragma: no cover
        user_id: UUID_ID
    else:
        user_id: GUID = GUID(null=False)


class DynamoDBAccessTokenDatabase(Generic[AP], AccessTokenDatabase[AP]):
    """Access token database adapter for AWS DynamoDB using aiopynamodb."""

    access_token_table: type[AP]

    def __init__(self, access_token_table: type[AP]):
        self.access_token_table = access_token_table

    async def get_by_token(
        self,
        token: str,
        max_age: datetime | None = None,
        instant_update: bool = False,
    ) -> AP | None:
        """Retrieve an access token by token string."""
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
        """Create a new access token and return an instance of AP."""
        await ensure_tables_exist(self.access_token_table)  # type: ignore

        if isinstance(create_dict, dict):
            token = self.access_token_table(**create_dict)
        else:
            token = create_dict
        try:
            await token.save(condition=self.access_token_table.token.does_not_exist())  # type: ignore
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "Access token could not be created because it already exists."
                ) from e
            raise ValueError(
                "Access token could not be created because the table does not exist."
            ) from e
        return token

    async def update(self, access_token: AP, update_dict: dict[str, Any]) -> AP:
        """Update an existing access token."""
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
            raise ValueError(
                "Access token could not be updated because the table does not exist."
            ) from e

    async def delete(self, access_token: AP) -> None:
        """Delete an access token."""
        await ensure_tables_exist(self.access_token_table)  # type: ignore

        try:
            await access_token.delete(condition=self.access_token_table.token.exists())  # type: ignore
        except DeleteError as e:
            raise ValueError(  # pragma: no cover
                "Access token could not be deleted."
            ) from e
        except PutError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                raise ValueError(
                    "Access token could not be deleted because it does not exist."
                ) from e
