"""BrokerConfig: per-broker configuration consumed by ``FastStreamPlugin``.

This module used to define ``BrokerPlugin``, which implemented Litestar's
``InitPluginProtocol``/``CLIPluginProtocol`` directly. The library now follows
the one-plugin / many-configs shape from ``litestar-saq``: a single
``FastStreamPlugin`` drives Litestar wiring, and ``BrokerConfig`` instances
carry per-broker data + lifecycle logic invoked by the host plugin.
"""

import contextlib
import logging
import types
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, cast

from litestar.config.app import AppConfig

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase

from ._helpers import qualname
from .di import build_broker_provide
from .discovery import collect
from .lifespan import LifespanComposer

logger = logging.getLogger(__name__)


class BrokerConfig:
    """Configure a FastStream broker for use under ``FastStreamPlugin``.

    Pass instances of this class via ``FastStreamConfig.brokers``.
    ``FastStreamPlugin`` invokes the lifecycle methods on each entry;
    ``BrokerConfig`` is not a Litestar plugin and must not be passed to
    ``Litestar(plugins=...)``.
    """

    def __init__(
        self,
        broker: "BrokerUsecase[Any, Any]",
        *,
        name: str | None = None,
        handlers: Sequence[Callable[..., Any]] = (),
        strict: bool = False,
        publish_only: bool = False,
    ) -> None:
        self.broker = broker
        self.name = name or broker.__class__.__name__.removesuffix("Broker").lower()
        self.handlers = list(handlers)
        self.strict = strict
        self.publish_only = publish_only
        self._composer = LifespanComposer(
            broker,
            state_key=self.name,
            publish_only=publish_only,
        )
        self._registered_subscribers: list[
            tuple[str, tuple[Any, ...], dict[str, Any]]
        ] = []
        self._registered_response_publishers: list[
            tuple[str, tuple[Any, ...], dict[str, Any]]
        ] = []
        # Controller-scoped markers; deferred to lifespan so binding uses
        # Litestar's own Controller singleton. See _bind_and_register_controllers.
        # Second tuple element is the unbound function (already unwrapped from
        # any ``BaseRouteHandler`` at discovery time), not the method name, so
        # methods decorated with both ``@get`` and ``@subscriber`` bind to the
        # user coroutine rather than Litestar's route-handler wrapper.
        self._pending_controller_subscribers: list[
            tuple[
                type,
                Callable[..., Any],
                list[tuple[tuple[Any, ...], dict[str, Any]]],
            ]
        ] = []
        self._pending_controller_response_publishers: list[
            tuple[type, Callable[..., Any], tuple[Any, ...], dict[str, Any]]
        ] = []
        self._pre_startup_hooked = False
        self._bound = False

    def after_startup(
        self,
        fn: Callable[[Any], Awaitable[None]],
    ) -> Callable[[Any], Awaitable[None]]:
        self._composer.add_after_startup(fn)
        return fn

    def on_broker_shutdown(
        self,
        fn: Callable[[Any], Awaitable[None]],
    ) -> Callable[[Any], Awaitable[None]]:
        self._composer.add_on_broker_shutdown(fn)
        return fn

    def _apply_to_app_config(self, app_config: AppConfig) -> AppConfig:  # noqa: PLR0915
        # Litestar may default `route_handlers` to None; normalize to a list
        # so we can safely mutate it (e.g. append route handlers).
        app_config.route_handlers = list(app_config.route_handlers or [])

        # Idempotency guard: if invoked a second time (e.g. an app being
        # re-constructed in a test fixture), skip broker binding so
        # publishers aren't double-registered. Lifespan + DI still re-run
        # because they target the fresh AppConfig.
        first_run = not self._bound
        self._bound = True

        result = collect(
            route_handlers=app_config.route_handlers,
            extra_handlers=self.handlers,
            plugin_name=self.name,
        )

        self._registered_subscribers.clear()
        self._registered_response_publishers.clear()
        self._pending_controller_subscribers = list(result.controller_subscribers)
        self._pending_controller_response_publishers = list(
            result.controller_response_publishers,
        )
        if (
            self._pending_controller_subscribers
            or self._pending_controller_response_publishers
        ) and not self._pre_startup_hooked:
            self._composer.add_pre_startup(self._bind_and_register_controllers)
            self._pre_startup_hooked = True

        # FastStream's @broker.subscriber and @broker.publisher both wrap the
        # given callable in a HandlerCallWrapper (via ensure_call_wrapper) and
        # attach themselves to that wrapper. For ``@publisher`` to actually
        # publish a subscriber's return value, BOTH must end up on the *same*
        # wrapper -- which means the second call has to receive the wrapper
        # the first call produced, not the raw function. Track wrappers by
        # ``id(fn)`` so both loops cooperate regardless of evaluation order.
        wrappers: dict[int, Any] = {}

        for fn, sub_specs in result.subscribers:
            for args, kwargs in sub_specs:
                if self._already_bound(fn, args):
                    logger.debug(
                        "skipping Tier-2 subscriber for %s; already bound by Tier-1",
                        fn,
                    )
                    continue
                if first_run:
                    handler = wrappers.get(id(fn), fn)
                    wrapper = self.broker.subscriber(*args, **kwargs)(handler)
                    wrappers[id(fn)] = wrapper
                    # Expose the HandlerCallWrapper's mock on the user's
                    # function so test code can introspect via `fn.mock` --
                    # mirrors the FastAPI router's `@router.subscriber`.
                    with contextlib.suppress(AttributeError, TypeError):
                        cast("Any", fn).mock = wrapper.mock
                self._registered_subscribers.append((
                    qualname(fn),
                    tuple(args),
                    dict(kwargs),
                ))

        for fn, args, kwargs in result.response_publishers:
            if first_run:
                handler = wrappers.get(id(fn), fn)
                wrappers[id(fn)] = self.broker.publisher(*args, **kwargs)(handler)
            self._registered_response_publishers.append((
                qualname(fn),
                tuple(args),
                dict(kwargs),
            ))

        if app_config.lifespan is None:
            app_config.lifespan = []  # pyrefly: ignore[bad-assignment]
        if not isinstance(app_config.lifespan, list):
            app_config.lifespan = list(app_config.lifespan)
        app_config.lifespan.append(self._composer.build())

        if self.publish_only:
            subscriber_count = len(self._registered_subscribers) + len(
                self._pending_controller_subscribers,
            )
            if subscriber_count:
                logger.warning(
                    "BrokerConfig(name=%r, publish_only=True): %d subscriber(s) "
                    "registered but will not consume in this process. "
                    "broker.start() is skipped; run `litestar faststream run` (or "
                    "another process without publish_only) to consume.",
                    self.name,
                    subscriber_count,
                )

        deps = dict(app_config.dependencies or {})
        # Register under the broker config's name (defaults to broker class
        # name lower-cased, e.g. ``rabbit``). Handlers can opt-in by declaring
        # a parameter of that name. We deliberately do NOT register under the
        # literal key ``"broker"`` because that collides across multiple
        # ``BrokerConfig`` instances. Users either annotate with the broker
        # class (resolved via ``signature_namespace``) or use the typed helpers
        # like ``RabbitBroker = Annotated[RB, Context("broker")]``.
        if self.name in deps:
            logger.warning(
                "Litestar dependency %r already registered; BrokerConfig will "
                "not override. Handlers expecting the broker via this name "
                "will receive the pre-existing dependency. Pass name='...' "
                "to BrokerConfig to disambiguate.",
                self.name,
            )
        else:
            deps[self.name] = build_broker_provide(self.broker)
        app_config.dependencies = deps

        # Extend signature_namespace so type annotations like ``RabbitBroker``
        # resolve at signature-parsing time without explicit imports in user
        # handler modules.
        ns = dict(app_config.signature_namespace or {})
        broker_cls_name = type(self.broker).__name__
        if broker_cls_name in ns:
            logger.debug(
                "signature_namespace already has %r; BrokerConfig will not override",
                broker_cls_name,
            )
        else:
            ns[broker_cls_name] = type(self.broker)
        app_config.signature_namespace = ns

        # Capture Litestar's logging_config so the CLI worker process can
        # re-apply standard-lib logging matching the HTTP server format.
        self._logging_config = app_config.logging_config

        return app_config

    def _apply_to_cli(self, cli: Any) -> None:
        from .cli import register_broker_cli

        register_broker_cli(cli, self)

    def _already_bound(self, fn: Any, args: tuple[Any, ...]) -> bool:
        """Identity-based dedup of Tier-2 subscribers vs Tier-1 broker subscribers.

        Same function bound twice for the same target is suppressed. Same
        function bound to *different* destinations is kept distinct: the
        Litestar Tier-2 marker carries ``args`` (queue/topic spec); when the
        broker subscriber exposes a comparable hint (``_extra_args``, ``args``,
        or ``queue``), require it to match before dedup'ing.

        When no queue-name hint can be extracted from the broker subscriber, we
        fall through to ``return False`` so the Tier-2 registration proceeds.
        Rationale: silently skipping a legitimate registration is harder to
        debug than accepting a (rare) double-bind. FastStream brokers do not
        currently raise on duplicate ``broker.subscriber`` calls for the same
        target; if they grow that behavior later, the duplicate will surface
        with a clear error from the broker itself.
        """
        existing = getattr(self.broker, "_subscribers", None) or []
        target = fn.__func__ if hasattr(fn, "__func__") else fn
        for sub in existing:
            sub_fn = getattr(sub, "fn", None)
            if sub_fn is None:
                calls = getattr(sub, "calls", None)
                if calls:
                    sub_fn = getattr(calls[0], "handler", None)
            if sub_fn is None:
                sub_fn = getattr(sub, "_call", None)
            if sub_fn is not target:
                continue
            sub_args = getattr(sub, "_extra_args", None) or getattr(sub, "args", None)
            if sub_args is not None:
                if sub_args == args:
                    return True
                continue
            # Fallback: try to match against ``queue`` attribute when ``args``
            # is a single-element queue spec.
            if args and len(args) == 1:
                queue_obj = getattr(sub, "queue", None)
                queue_name = getattr(queue_obj, "name", queue_obj)
                if queue_name is not None and queue_name == args[0]:
                    return True
                continue
            # No reliable queue-name hint on this candidate: keep scanning
            # the remaining subscribers. A later candidate may still match;
            # only conclude "not bound" once every candidate is exhausted.
            continue
        return False

    def _collect_controller_instances(self, app: Any) -> dict[type, Any]:
        """Find the Controller singletons Litestar instantiated.

        Walks ``app.routes`` and recovers each unique Controller instance via
        the ``__self__`` of its bound route handler methods. Controllers with
        no HTTP route handlers are *not* instantiated by Litestar in a way
        we can reach (the instance is created during ``Controller.as_router``
        and immediately discarded); those Controllers fall through to the
        ``__new__``-then-``__init__`` instantiation in
        ``_bind_and_register_controllers``.
        """
        instances: dict[type, Any] = {}
        for route in app.routes:
            for rh in getattr(route, "route_handlers", []) or []:
                fn = getattr(rh, "fn", None)
                owner = getattr(fn, "__self__", None)
                if owner is None:
                    continue
                cls = type(owner)
                instances.setdefault(cls, owner)
        return instances

    def _resolve_controller_instance(
        self,
        controller_cls: type,
        cache: dict[type, Any],
    ) -> Any:
        """Return the singleton instance for ``controller_cls``.

        Uses Litestar's instance when reachable (mixed HTTP+stream
        Controller); otherwise creates one with ``owner=None`` and caches it
        for the lifetime of this plugin instance. Stream-only Controllers
        only ever have a single instance either way.
        """
        if controller_cls in cache:
            return cache[controller_cls]
        # owner is only used for HTTP routing; stream-only Controllers never
        # touch it. ``Controller.__init__`` requires the kwarg but stores
        # it without further dispatch.
        instance = controller_cls(owner=None)
        cache[controller_cls] = instance
        return instance

    async def _bind_and_register_controllers(self, app: Any) -> None:
        """Pre-startup hook: bind Controller methods, register with broker.

        Runs after Litestar has built ``app.routes`` but before the broker's
        ``connect()/start()``. Looks up the Controller singleton instance
        Litestar already created (so HTTP handlers and stream subscribers
        share state via ``self``), or instantiates one for stream-only
        Controllers, and registers each marked method as a bound method on
        the broker.
        """
        cache = self._collect_controller_instances(app)

        # Bind each (controller_cls, unbound) ONCE so subscriber and publisher
        # passes can share the resulting HandlerCallWrapper. ``types.MethodType``
        # returns a fresh bound method each call, which would otherwise hand
        # FastStream two different objects and create two disjoint wrappers --
        # the same root cause as the staticmethod path.
        bound_methods: dict[tuple[type, Any], Any] = {}

        def _get_bound(controller_cls: type, unbound: Any) -> Any:
            key = (controller_cls, unbound)
            if key not in bound_methods:
                instance = self._resolve_controller_instance(controller_cls, cache)
                bound_methods[key] = types.MethodType(unbound, instance)
            return bound_methods[key]

        wrappers: dict[int, Any] = {}

        for controller_cls, unbound, sub_specs in self._pending_controller_subscribers:
            bound = _get_bound(controller_cls, unbound)
            for args, kwargs in sub_specs:
                if self._already_bound(bound, args):
                    logger.debug(
                        "skipping Tier-2 subscriber for %s.%s; already bound",
                        controller_cls.__qualname__,
                        getattr(unbound, "__name__", repr(unbound)),
                    )
                    continue
                handler = wrappers.get(id(bound), bound)
                wrapper = self.broker.subscriber(*args, **kwargs)(handler)
                wrappers[id(bound)] = wrapper
                # Mirror the non-Controller path: expose ``mock`` on the
                # class-level function so tests can introspect via
                # ``ControllerCls.method_name.mock`` (matching the FastAPI
                # router's ``@router.subscriber`` convention).
                with contextlib.suppress(AttributeError, TypeError):
                    cast("Any", unbound).mock = cast("Any", wrapper).mock
                self._registered_subscribers.append((
                    qualname(bound),
                    tuple(args),
                    dict(kwargs),
                ))

        for (
            controller_cls,
            unbound,
            args,
            kwargs,
        ) in self._pending_controller_response_publishers:
            bound = _get_bound(controller_cls, unbound)
            handler = wrappers.get(id(bound), bound)
            wrappers[id(bound)] = self.broker.publisher(*args, **kwargs)(
                cast("Callable[..., Any]", handler),
            )
            self._registered_response_publishers.append((
                qualname(bound),
                tuple(args),
                dict(kwargs),
            ))
