import pytest
from aiopynamodb.models import Model

from fastapi_users_db_dynamodb import DynamoDBBaseUserTable, config
from fastapi_users_db_dynamodb._generics import classproperty
from fastapi_users_db_dynamodb.access_token import DynamoDBBaseAccessTokenTable
from fastapi_users_db_dynamodb.attributes import GUID, TransformingUnicodeAttribute
from fastapi_users_db_dynamodb.tables import delete_tables, ensure_tables_exist

from .conftest import hash_string


class NotAModel:
    """A class representing an invalid `Model`."""

    pass


class IncompleteModel(Model):
    """A class representing an incomplete `Model`, which misses required functions."""

    pass


class ValidModel(Model):
    """A class representing a valid `Model`."""

    attr = TransformingUnicodeAttribute(transform=str.lower)
    attr2 = TransformingUnicodeAttribute(transform=hash_string)

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
            return "valid_model_test"

        @classproperty
        def region(self) -> str:
            return config.get("DATABASE_REGION")

        @classproperty
        def billing_mode(self) -> str:
            return config.get("DATABASE_BILLING_MODE").value


@pytest.mark.asyncio
async def test_tables_invalid_models(monkeypatch):
    """Test table creation on various `Model` instances.

    Args:
        monkeypatch (_type_): The `pytest` monkeypatcher which is used to modify models.
    """
    with pytest.raises(TypeError, match="must be a subclass of Model"):
        await ensure_tables_exist(NotAModel)  # type: ignore

    with pytest.raises(AttributeError, match="PynamoDB Models require a"):
        await ensure_tables_exist(IncompleteModel)

    with pytest.raises(AttributeError, match="PynamoDB Models require a"):
        await delete_tables(IncompleteModel)

    await ensure_tables_exist(ValidModel)
    assert await ValidModel.exists()
    await delete_tables(ValidModel)
    assert not await ValidModel.exists()

    monkeypatch.delattr(Model, "exists", raising=True)
    with pytest.raises(TypeError):
        await ensure_tables_exist(IncompleteModel)


def test_config(monkeypatch):
    """Test config settings and changes.

    Args:
        monkeypatch (_type_): The `pytest` monkeypatcher which is used to modify models.
    """
    billing_mode = config.BillingMode.PAY_PER_REQUEST
    assert billing_mode.value == str(billing_mode)

    local_get, local_set = config.__create_config()
    monkeypatch.setattr(config, "get", local_get)
    monkeypatch.setattr(config, "set", local_set)

    with pytest.raises(KeyError, match="Unknown config key"):
        config.set("non_existent_key", "some_value")

    with pytest.raises(TypeError, match="Invalid type for"):
        config.set("DATABASE_BILLING_MODE", 1001)

    region = "us-east-1"
    config.set("DATABASE_REGION", region)
    assert config.get("DATABASE_REGION") == region

    # Test Meta definitions
    assert DynamoDBBaseUserTable.Meta.table_name == config.get(
        "DATABASE_USERTABLE_NAME"
    )
    assert DynamoDBBaseAccessTokenTable.Meta.table_name == config.get(
        "DATABASE_TOKENTABLE_NAME"
    )
    assert (
        DynamoDBBaseUserTable.Meta.region
        == DynamoDBBaseAccessTokenTable.Meta.region
        == config.get("DATABASE_REGION")
    )
    assert (
        DynamoDBBaseUserTable.Meta.billing_mode
        == DynamoDBBaseAccessTokenTable.Meta.billing_mode
        == config.get("DATABASE_BILLING_MODE").value
    )


def test_attributes(user_id):
    """Test serialization and deserialization of `Attribute` instances.

    Args:
        user_id (_type_): The default user id to use for testing.
    """
    id = GUID()
    assert id.serialize(None) is None

    user_id_str = str(user_id)
    assert user_id_str == id.serialize(user_id_str)

    assert id.deserialize(None) is None
    assert user_id == id.deserialize(user_id)

    assert ValidModel().attr is None
    model = ValidModel(attr="TEST", attr2="TEST")
    assert model.attr == "test"
    model.attr = "ANOTHER TEST"
    assert model.attr == "another test"
    assert model.attr2 == hash_string("TEST")
    model.attr2 = "ANOTHER TEST"
    assert model.attr2 == hash_string("ANOTHER TEST")
