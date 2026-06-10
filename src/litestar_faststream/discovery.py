"""Walk Litestar `route_handlers` and explicit handler/module lists.

Collects FastStream marker metadata and emits a structured result.

``@subscriber`` / ``@publisher`` mark *broker* handlers. They cannot
decorate a Litestar HTTP route handler — if a method carrying ``@get`` /
``@post`` / etc. also has these markers, app init fails with
``MarkerConfigurationError``. HTTP handlers that need to publish should
inject the broker via Litestar DI and call ``broker.publish(...)`` directly.
"""

import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from litestar import Controller
from litestar.handlers.base import BaseRouteHandler

from .exceptions import HandlerDiscoveryError, MarkerConfigurationError


@dataclass
class DiscoveryResult:
    subscribers: list[
        tuple[
            Callable[..., Any],
            list[tuple[tuple[Any, ...], dict[str, Any]]],
        ]
    ] = field(default_factory=list)
    response_publishers: list[
        tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]
    ] = field(default_factory=list)
    # Controller-class markers — binding deferred to lifespan so that the
    # registered callable is a method bound to Litestar's own Controller
    # singleton (the same instance used by HTTP handlers). See plugin.py's
    # pre-startup hook for the late binding pass.
    #
    # We persist the **unbound function** (already extracted from any
    # ``BaseRouteHandler`` wrapper at discovery time) rather than the method
    # name. Otherwise, when a Controller method stacks ``@get(...)`` and
    # ``@subscriber(...)``, looking the name back up on the class would yield
    # the Litestar route-handler wrapper instead of the user's coroutine —
    # binding *that* to ``self`` makes the broker dispatch to the wrong
    # callable.
    controller_subscribers: list[
        tuple[
            type,
            Callable[..., Any],
            list[tuple[tuple[Any, ...], dict[str, Any]]],
        ]
    ] = field(default_factory=list)
    controller_response_publishers: list[
        tuple[type, Callable[..., Any], tuple[Any, ...], dict[str, Any]]
    ] = field(default_factory=list)


def collect(
    route_handlers: Sequence[Any],
    extra_handlers: Sequence[Callable[..., Any]],
    plugin_name: str | None = None,
) -> DiscoveryResult:
    seen: set[int] = set()
    result = DiscoveryResult()

    for h in route_handlers:
        _walk_handler(h, result, seen, plugin_name)

    for fn in extra_handlers:
        if not _has_any_marker(fn):
            msg = (
                f"BrokerConfig.handlers includes {fn!r} but no "
                f"@subscriber/@publisher marker found."
            )
            raise HandlerDiscoveryError(msg)
        _inspect_callable(fn, result, seen, plugin_name=plugin_name)

    return result


def _walk_handler(
    h: Any,
    result: DiscoveryResult,
    seen: set[int],
    plugin_name: str | None,
) -> None:
    if isinstance(h, type) and issubclass(h, Controller):
        # Controller route handlers live in the class ``__dict__`` as
        # ``BaseRouteHandler`` instances; iterate the MRO so inherited
        # handlers are picked up too. Plain ``@subscriber``-marked methods
        # (no Litestar HTTP decorator) are still functions, so a second pass
        # via ``inspect.getmembers`` covers them.
        for cls in h.__mro__:
            for member in vars(cls).values():
                if isinstance(member, BaseRouteHandler):
                    _reject_http_handler_markers(member)
        for name, member in inspect.getmembers(h, predicate=inspect.isfunction):
            # staticmethods take no ``self`` and so should be registered
            # eagerly (in ``on_app_init``) like a module-level function, not
            # deferred to lifespan binding. ``inspect.getmembers(..., isfunction)``
            # returns the underlying function for staticmethods and regular
            # methods identically — disambiguate by looking up the wrapper
            # in the class ``__dict__`` along the MRO.
            is_static = any(
                isinstance(base.__dict__.get(name), staticmethod)
                for base in h.__mro__
                if name in base.__dict__
            )
            _inspect_callable(
                member,
                result,
                seen,
                plugin_name=plugin_name,
                controller_cls=None if is_static else h,
            )
        return
    if isinstance(h, BaseRouteHandler):
        _reject_http_handler_markers(h)
        return
    nested = getattr(h, "route_handlers", None)
    if nested is not None:
        for n in nested:
            _walk_handler(n, result, seen, plugin_name)
        return
    if callable(h):
        _inspect_callable(h, result, seen, plugin_name=plugin_name)


def collect_marker_plugin_filters(
    route_handlers: Sequence[Any],
    extra_handlers: Sequence[Callable[..., Any]],
) -> set[str]:
    """Return the set of non-None ``plugin=`` filters present in markers.

    Used by ``FastStreamPlugin`` to warn when a marker references an
    unregistered broker name.
    """
    seen: set[int] = set()
    filters: set[str] = set()

    def _scan(fn: Any) -> None:
        target = fn.__func__ if hasattr(fn, "__func__") else fn
        if id(target) in seen:
            return
        seen.add(id(target))
        for attr in ("__faststream_subscribers__", "__faststream_publishers__"):
            for _args, kwargs in getattr(target, attr, None) or ():
                name = kwargs.get("plugin")
                if name is not None:
                    filters.add(name)

    def _walk(h: Any) -> None:
        if isinstance(h, type) and issubclass(h, Controller):
            for _name, member in inspect.getmembers(h, predicate=inspect.isfunction):
                _scan(member)
            return
        if isinstance(h, BaseRouteHandler):
            return
        nested = getattr(h, "route_handlers", None)
        if nested is not None:
            for n in nested:
                _walk(n)
            return
        if callable(h):
            _scan(h)

    for h in route_handlers:
        _walk(h)
    for fn in extra_handlers:
        _scan(fn)
    return filters


def _reject_http_handler_markers(route_handler: BaseRouteHandler) -> None:
    """Raise if an HTTP route handler also carries @subscriber/@publisher.

    Broker handlers and HTTP handlers are separate citizens; mixing them on
    a single method is a design error. HTTP handlers that need to publish
    should inject the broker via Litestar DI.
    """
    fn = getattr(route_handler, "fn", None)
    if fn is None:
        return
    target = fn.__func__ if hasattr(fn, "__func__") else fn
    offenders = [
        name
        for name in ("__faststream_subscribers__", "__faststream_publishers__")
        if getattr(target, name, None)
    ]
    if not offenders:
        return
    label = "/".join(o.strip("_").rsplit("_", 1)[0] for o in offenders)
    qualname = getattr(target, "__qualname__", repr(target))
    msg = (
        f"@{label} cannot decorate an HTTP route handler ({qualname}). "
        f"Broker handlers and HTTP handlers are separate — to publish from "
        f"an HTTP handler, inject the broker via Litestar DI (declare a "
        f"parameter matching BrokerConfig.name typed with the broker class) "
        f"and call broker.publish(...) directly."
    )
    raise MarkerConfigurationError(msg)


def _has_any_marker(fn: Any) -> bool:
    target = fn.__func__ if hasattr(fn, "__func__") else fn
    return any(
        getattr(target, attr, None)
        for attr in (
            "__faststream_subscribers__",
            "__faststream_publishers__",
        )
    )


def _claims(marker_plugin: str | None, plugin_name: str | None) -> bool:
    """Check whether a marker entry belongs to this plugin.

    Claimed when the marker has no plugin filter, when the caller has no
    plugin context (test/standalone usage), or when both names match.
    """
    return marker_plugin is None or plugin_name is None or marker_plugin == plugin_name


def _inspect_callable(
    fn: Any,
    result: DiscoveryResult,
    seen: set[int],
    *,
    plugin_name: str | None = None,
    controller_cls: type | None = None,
) -> None:
    target = fn.__func__ if hasattr(fn, "__func__") else fn
    if id(target) in seen:
        return
    seen.add(id(target))

    subs = getattr(target, "__faststream_subscribers__", None)
    if subs:
        filtered: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        for args, kwargs in subs:
            if not _claims(kwargs.get("plugin"), plugin_name):
                continue
            filtered.append((args, {k: v for k, v in kwargs.items() if k != "plugin"}))
        if filtered:
            if controller_cls is not None:
                result.controller_subscribers.append((controller_cls, target, filtered))
            else:
                result.subscribers.append((target, filtered))

    pubs = getattr(target, "__faststream_publishers__", None)
    if pubs:
        for args, kwargs in pubs:
            if not _claims(kwargs.get("plugin"), plugin_name):
                continue
            entry_kwargs = {k: v for k, v in kwargs.items() if k != "plugin"}
            if controller_cls is not None:
                result.controller_response_publishers.append((
                    controller_cls,
                    target,
                    args,
                    entry_kwargs,
                ))
            else:
                result.response_publishers.append((target, args, entry_kwargs))
