"""FastAPI Users DynamoDB generics."""

import uuid
from datetime import datetime, timezone

UUID_ID = uuid.UUID


def now_utc() -> datetime:
    """
    Returns the current time in UTC with timezone awareness.
    Equivalent to the old implementation.
    """
    return datetime.now(timezone.utc)
