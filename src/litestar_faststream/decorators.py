"""Free decorators for FastStream Litestar integration.

These decorators stash metadata on the wrapped function. The plugin's
discovery pass walks `app_config.route_handlers` at `on_app_init` and
binds marked callables to the broker.
"""

from collections.abc import Callable
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def _set_marker(fn: Any, attr: str, entry: Any, decorator_name: str) -> None:
    existing = getattr(fn, attr, [])
    try:
        setattr(fn, attr, [*existing, entry])
    except (AttributeError, TypeError) as exc:
        msg = (
            f"@{decorator_name} can only decorate writable callables; "
            f"{fn!r} rejected attribute assignment ({exc})."
        )
        raise TypeError(msg) from exc


def subscriber(
    *args: Any,
    plugin: str | None = None,
    **kwargs: Any,
) -> Callable[[F], F]:
    def deco(fn: F) -> F:
        entry_kwargs = {**kwargs, "plugin": plugin} if plugin is not None else kwargs
        _set_marker(
            fn,
            "__faststream_subscribers__",
            (args, entry_kwargs),
            "subscriber",
        )
        return fn

    return deco


def publisher(
    *args: Any,
    plugin: str | None = None,
    **kwargs: Any,
) -> Callable[[F], F]:
    def deco(fn: F) -> F:
        entry_kwargs = {**kwargs, "plugin": plugin} if plugin is not None else kwargs
        _set_marker(
            fn,
            "__faststream_publishers__",
            (args, entry_kwargs),
            "publisher",
        )
        return fn

    return deco
