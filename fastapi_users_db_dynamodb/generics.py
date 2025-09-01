"""FastAPI Users DynamoDB generics for UUID and timestamp handling.

This module replaces SQLAlchemy-specific TypeDecorators with DynamoDB-friendly
helpers while keeping the same public API for compatibility.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import UUID4


class GUID(UUID4):
    """
    Platform-independent GUID type.

    Kept for API compatibility with the old SQLAlchemy-based code.
    In DynamoDB, this behaves as a lightweight UUID validator/converter.
    """

    python_type = UUID4

    def __init__(self, *args, **kwargs):
        """DynamoDB does not need type decorators, but we mimic SQLAlchemy API."""
        pass

    @staticmethod
    def to_storage(value: uuid.UUID | str | None) -> str | None:
        """Convert UUID or string to a DynamoDB-storable string."""
        if value is None:
            return None
        return str(value) if isinstance(value, uuid.UUID) else str(uuid.UUID(value))

    @staticmethod
    def from_storage(value: str | uuid.UUID | None) -> uuid.UUID | None:
        """Convert a stored string back into a UUID object."""
        if value is None:
            return None
        return value if isinstance(value, uuid.UUID) else uuid.UUID(value)


def now_utc() -> datetime:
    """
    Returns the current time in UTC with timezone awareness.
    Equivalent to the old implementation.
    """
    return datetime.now(timezone.utc)


class TIMESTAMPAware(datetime):
    """
    Kept for API compatibility.

    In SQLAlchemy, this handled database-specific timestamp behavior.
    In DynamoDB, timestamps are stored as ISO 8601 strings and always
    returned as timezone-aware datetimes.
    """

    python_type = datetime

    def __init__(self, *args, **kwargs):
        """DynamoDB does not require dialect-level timestamp handling."""
        pass

    @staticmethod
    def to_storage(value: datetime | None) -> str | None:
        """Convert datetime to an ISO 8601 string for DynamoDB storage."""
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()

    @staticmethod
    def from_storage(value: str | datetime | None) -> datetime | None:
        """Convert stored ISO 8601 string to timezone-aware datetime."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
