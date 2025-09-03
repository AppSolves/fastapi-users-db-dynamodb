import pytest
from aiopynamodb.models import Model

from fastapi_users_db_dynamodb import config
from fastapi_users_db_dynamodb.attributes import GUID
from fastapi_users_db_dynamodb.tables import ensure_tables_exist


class NotAModel:
    pass


class IncompleteModel(Model):
    pass


@pytest.mark.asyncio
async def test_tables_invalid_models(monkeypatch):
    with pytest.raises(TypeError, match="must be a subclass of Model"):
        await ensure_tables_exist(NotAModel)  # type: ignore

    with pytest.raises(AttributeError, match="PynamoDB Models require a"):
        await ensure_tables_exist(IncompleteModel)

    monkeypatch.delattr(Model, "exists", raising=True)
    with pytest.raises(TypeError):
        await ensure_tables_exist(IncompleteModel)


def test_config(monkeypatch):
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


def test_attributes(user_id):
    id = GUID()
    assert id.serialize(None) is None

    user_id_str = str(user_id)
    assert user_id_str == id.serialize(user_id_str)

    assert id.deserialize(None) is None
    assert user_id == id.deserialize(user_id)
