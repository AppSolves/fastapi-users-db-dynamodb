"""FastAPI Users DynamoDB generics."""

import uuid
from datetime import UTC, datetime

UUID_ID = uuid.UUID


class classproperty:  # pragma: no cover
    """A decorator which behaves like `@property`, but for classmethods.
    This allows to define read-only class properties.
    Example::
        ```python
        class Foo:
            @classproperty
            def bar(cls):
                return 42 \

        Foo.bar # calls __get__ \

        Foo.bar = 99 # does NOT call __set__ on classproperty, just sets Foo.bar to 99 \

        del Foo.bar # does NOT call __delete__ on classproperty, just deletes Foo.bar
        ```
    """

    def __init__(self, fget=None, fset=None, fdel=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        self.__doc__ = getattr(fget, "__doc__", None)

    def __get__(self, instance, owner):
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(owner)

    def __set__(self, instance, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        cls = type(instance) if instance is not None else instance
        if cls is None:  # when instance is None, need to use owner
            raise AttributeError("class not found for setting")
        return self.fset(cls, value)

    def __delete__(self, instance):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")
        cls = type(instance) if instance is not None else instance
        if cls is None:
            raise AttributeError("class not found for deletion")
        return self.fdel(cls)

    def getter(self, fget):
        return type(self)(fget, self.fset, self.fdel)

    def setter(self, fset):
        return type(self)(self.fget, fset, self.fdel)

    def deleter(self, fdel):
        return type(self)(self.fget, self.fset, fdel)


def now_utc() -> datetime:
    """
    Returns the current time in UTC with timezone awareness.
    Equivalent to the old implementation.
    """
    return datetime.now(UTC)
