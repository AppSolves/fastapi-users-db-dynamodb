"""FastAPI Users DynamoDB generics."""

import uuid
from datetime import UTC, datetime

UUID_ID = uuid.UUID


def now_utc() -> datetime:
    """
    Returns the current time in UTC with timezone awareness.
    Equivalent to the old implementation.
    """
    return datetime.now(UTC)
