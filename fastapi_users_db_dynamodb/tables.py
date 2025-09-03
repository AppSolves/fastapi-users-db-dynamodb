from __future__ import annotations

from types import CoroutineType
from typing import Any, Protocol

from aiopynamodb.models import Model

from . import config

__tables_cache: set[type[CreatableTable]] = set()


class CreatableTable(Protocol):
    @classmethod
    async def exists(cls) -> CoroutineType[Any, Any, bool] | bool: ...

    @classmethod
    async def create_table(
        cls,
        *,
        wait: bool = ...,
        billing_mode: str | None = ...,
    ) -> CoroutineType[Any, Any, Any] | Any: ...


def _check_creatable_table(cls: type[Any]):
    if not issubclass(cls, Model):
        raise TypeError(f"{cls.__name__} must be a subclass of Model")
    if not hasattr(cls, "exists") or not hasattr(cls, "create_table"):
        raise TypeError(f"{cls.__name__} must implement exists() and create_table()")


async def ensure_tables_exist(*tables: type[CreatableTable]) -> None:
    """
    Ensure that all given DynamoDB tables exist.
    Will be called automatically from the DB instance.
    """
    global __tables_cache

    for table_cls in tables:
        _check_creatable_table(table_cls)
        if table_cls not in __tables_cache:
            if not await table_cls.exists():
                await table_cls.create_table(
                    billing_mode=config.get("DATABASE_BILLING_MODE").value,
                    wait=True,
                )
            __tables_cache.add(table_cls)
