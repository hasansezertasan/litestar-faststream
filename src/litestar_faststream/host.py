"""FastStreamPlugin: the sole Litestar plugin for FastStream integration.

Mirrors the ``litestar-saq`` shape: one plugin (``FastStreamPlugin``) consumes
a single config (``FastStreamConfig``) containing N ``BrokerConfig`` entries.
The plugin:

* delegates per-broker lifecycle to each :class:`BrokerConfig` entry
  (discovery, broker binding, lifespan composition, DI registration);
* optionally serves a combined AsyncAPI document built via
  ``AsyncAPI(*all_brokers, ...)`` — FastStream's factory accepts multiple
  brokers natively, so combined output requires no custom merger;
* validates name collisions across hosted broker configs upfront.
"""

import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from faststream.specification.asyncapi.factory import AsyncAPI
from litestar.config.app import AppConfig
from litestar.plugins import CLIPluginProtocol, InitPluginProtocol

from .asyncapi import build_asyncapi_controller
from .discovery import collect_marker_plugin_filters
from .exceptions import (
    BrokerConfigurationError,
    BrokerNotRegisteredError,
    DuplicateBrokerNameError,
    MarkerConfigurationError,
)
from .plugin import BrokerConfig

logger = logging.getLogger(__name__)


def _has_otel_plugin(app: Any) -> bool:
    """Return True if the Litestar app has ``OpenTelemetryPlugin`` registered.

    Uses Litestar 2.x's typed plugin registry (``app.plugins.get(type)``) so
    subclasses of ``OpenTelemetryPlugin`` also match. The import is wrapped
    in ``try/except ImportError`` because ``litestar-otel`` ships as an
    optional extra; absence of the submodule is "OTel not configured", which
    is the right answer for the auto-detection predicate.
    """
    try:
        from litestar.plugins.opentelemetry import OpenTelemetryPlugin
    except ImportError:
        return False
    try:
        return bool(app.plugins.get(OpenTelemetryPlugin))
    except Exception:
        return False


@dataclass
class FastStreamConfig:
    """Top-level configuration for :class:`FastStreamPlugin`.

    Holds the list of :class:`BrokerConfig` entries plus host-level metadata
    used to render the combined AsyncAPI document. Mirrors the
    ``SAQConfig`` / ``SAQPlugin`` split in ``litestar-saq``: the config is a
    pure data carrier; the plugin owns lifecycle.
    """

    brokers: Sequence[BrokerConfig]
    title: str | None = None
    description: str | None = None
    version: str | None = None
    tags: Sequence[Any] = ()
    asyncapi_url: str | None = None
    asyncapi_include_in_schema: bool = False
    enable_otel: bool | None = None
    """Auto-inject :class:`OtelMiddleware` into every broker.

    Tri-state, mirroring ``litestar-saq``'s ``SAQConfig.enable_otel``:

    - ``None`` (default): enable if ``opentelemetry`` is importable AND the
      Litestar app has an :class:`OpenTelemetryPlugin` configured.
    - ``True``: enable unconditionally; raise
      :class:`BrokerConfigurationError` at startup if ``opentelemetry``
      is not installed.
    - ``False``: never auto-inject. Users can still add the middleware
      manually for per-broker control.
    """

    def should_enable_otel(self, app: "Any | None" = None) -> bool:
        """Resolve :attr:`enable_otel` against the live Litestar app.

        Defers the decision to startup time (when ``app`` is available) so
        that another plugin's late registration of ``OpenTelemetryPlugin``
        is still observed.

        Raises:
            BrokerConfigurationError: If ``enable_otel=True`` is set but the
                ``opentelemetry`` package is not importable.
        """
        from ._otel_typing import OPENTELEMETRY_INSTALLED

        if self.enable_otel is True:
            if not OPENTELEMETRY_INSTALLED:
                msg = (
                    "FastStreamConfig.enable_otel=True but ``opentelemetry`` "
                    "is not installed. Install it (or set enable_otel=False)."
                )
                raise BrokerConfigurationError(msg)
            return True
        if self.enable_otel is False:
            return False
        if not OPENTELEMETRY_INSTALLED:
            return False
        if app is None:
            return False
        return _has_otel_plugin(app)


class FastStreamPlugin(InitPluginProtocol, CLIPluginProtocol):
    """Litestar plugin hosting one or more FastStream brokers.

    Takes a :class:`FastStreamConfig` whose ``brokers`` is a sequence of
    :class:`BrokerConfig` instances.
    """

    def __init__(self, config: FastStreamConfig) -> None:
        if not config.brokers:
            msg = "FastStreamPlugin requires at least one broker"
            raise BrokerConfigurationError(msg)

        for entry in config.brokers:
            if not isinstance(entry, BrokerConfig):
                msg = (
                    f"FastStreamConfig.brokers entries must be BrokerConfig "
                    f"instances; got {entry!r}"
                )
                raise BrokerConfigurationError(msg)

        self.config = config
        self._children: list[BrokerConfig] = list(config.brokers)
        self._validate_children()
        self._combined_schema: Any = None

    def _validate_children(self) -> None:
        seen_names: dict[str, BrokerConfig] = {}
        for child in self._children:
            if child.name in seen_names:
                msg = (
                    f"Duplicate broker name {child.name!r} in FastStreamPlugin. "
                    f"Pass a unique name to each BrokerConfig."
                )
                raise DuplicateBrokerNameError(msg)
            seen_names[child.name] = child

    # ----- hook decorators -----------------------------------------------

    def after_startup(
        self,
        name: str,
    ) -> Callable[[Callable[[Any], Awaitable[None]]], Callable[[Any], Awaitable[None]]]:
        """Return a decorator that registers an after-startup hook for broker ``name``.

        Two-step shape — the ``name`` selects the child config, the returned
        callable is the actual decorator::

            @plugin.after_startup("kafka")
            async def announce(app) -> None: ...
        """
        child = self._child(name)
        return child.after_startup

    def on_broker_shutdown(
        self,
        name: str,
    ) -> Callable[[Callable[[Any], Awaitable[None]]], Callable[[Any], Awaitable[None]]]:
        """Return a decorator that registers an on-shutdown hook for broker ``name``.

        Same two-step shape as :meth:`after_startup`.
        """
        child = self._child(name)
        return child.on_broker_shutdown

    def _child(self, name: str) -> BrokerConfig:
        for child in self._children:
            if child.name == name:
                return child
        registered = sorted(c.name for c in self._children)
        msg = (
            f"No broker named {name!r} registered on FastStreamPlugin; "
            f"registered: {registered}"
        )
        raise BrokerNotRegisteredError(msg)

    @property
    def brokers(self) -> tuple[BrokerConfig, ...]:
        """Tuple of internal BrokerConfig children (read-only view)."""
        return tuple(self._children)

    # ----- Litestar plugin hooks -----------------------------------------

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        app_config.route_handlers = list(app_config.route_handlers or [])

        self._warn_unknown_plugin_filters(app_config)

        # Fail fast on the True-without-OTel case so users learn at app init
        # rather than at lifespan startup. The auto-detect (None) path is
        # resolved at startup when the live Litestar instance is available.
        if self.config.enable_otel is True:
            from ._otel_typing import OPENTELEMETRY_INSTALLED

            if not OPENTELEMETRY_INSTALLED:
                msg = (
                    "FastStreamConfig.enable_otel=True but ``opentelemetry`` "
                    "is not installed."
                )
                raise BrokerConfigurationError(msg)

        for child in self._children:
            app_config = child._apply_to_app_config(app_config)

        self._register_brokers_dependency(app_config)
        self._register_otel_resolution()

        if self.config.asyncapi_url:
            ctrl = self._build_combined_asyncapi()
            if ctrl is not None:
                app_config.route_handlers.append(ctrl)
        return app_config

    def _register_otel_resolution(self) -> None:
        """Resolve and inject ``OtelMiddleware`` at lifespan startup.

        Hooks into the *first* child's ``LifespanComposer`` rather than every
        child, so the per-app resolution runs exactly once even with N
        brokers. The hook receives the live Litestar app, which is the only
        time ``app.plugins.get(OpenTelemetryPlugin)`` returns a meaningful
        answer.
        """
        if self.config.enable_otel is False:
            return  # explicit opt-out; nothing to wire
        if not self._children:
            return
        self._children[0]._composer.add_pre_startup(self._install_otel_if_enabled)

    async def _install_otel_if_enabled(self, app: Any) -> None:
        if not self.config.should_enable_otel(app):
            return
        from .instrumentation import OtelMiddleware

        for child in self._children:
            broker = child.broker
            existing = list(getattr(broker, "middlewares", None) or [])
            # Idempotency: ``middlewares`` may hold middleware classes OR
            # instances. Treat a class match (``is OtelMiddleware``) and an
            # isinstance match (``isinstance(m, OtelMiddleware)``) both as
            # "already installed" so we never double-wrap.
            already = any(
                m is OtelMiddleware or isinstance(m, OtelMiddleware) for m in existing
            )
            if already:
                logger.debug(
                    "OtelMiddleware already present on broker %r; skipping",
                    child.name,
                )
                continue
            broker.add_middleware(OtelMiddleware)
            logger.info(
                "Auto-injected OtelMiddleware into broker %r (enable_otel=%r)",
                child.name,
                self.config.enable_otel,
            )

    def _register_brokers_dependency(self, app_config: AppConfig) -> None:
        """Expose a ``Brokers`` registry under the literal DI key ``"brokers"``.

        Lets handlers that pick a broker at runtime write
        ``brokers.get("rabbit").publish(...)`` instead of declaring N typed
        parameters. The per-broker named dependencies registered by each
        ``BrokerConfig._apply_to_app_config`` are still there -- this is an
        additional access path, not a replacement.

        If a user dependency already owns ``"brokers"``, warn and leave it
        alone; the per-broker DI keys keep working regardless.
        """
        from .di import Brokers, build_brokers_provide

        deps = dict(app_config.dependencies or {})
        if "brokers" in deps:
            logger.warning(
                "Litestar dependency 'brokers' already registered; "
                "FastStreamPlugin will not override. Use per-broker "
                "dependencies (one per BrokerConfig name) instead.",
            )
            return
        registry = Brokers({c.name: c.broker for c in self._children})
        deps["brokers"] = build_brokers_provide(registry)
        app_config.dependencies = deps

        ns = dict(app_config.signature_namespace or {})
        ns.setdefault("Brokers", Brokers)
        app_config.signature_namespace = ns

    def on_cli_init(self, cli: Any) -> None:
        for child in self._children:
            child._apply_to_cli(cli)

    # ----- unknown-marker warning ----------------------------------------

    def _warn_unknown_plugin_filters(self, app_config: AppConfig) -> None:
        """Warn (or raise if any child is strict) on marker names not registered.

        Aggregates ``handlers`` and ``strict`` across all child configs and
        scans route handlers / extra handlers for ``plugin=`` filters that
        reference unknown broker names.

        Raises:
            MarkerConfigurationError: When any child has ``strict=True`` and
                a marker references an unknown broker name.
        """
        registered = {child.name for child in self._children}
        all_handlers: list[Any] = []
        strict = False
        for child in self._children:
            all_handlers.extend(child.handlers)
            strict = strict or child.strict
        used = collect_marker_plugin_filters(
            route_handlers=app_config.route_handlers,
            extra_handlers=all_handlers,
        )
        unknown = used - registered
        if not unknown:
            return
        if strict:
            msg = (
                f"FastStream marker(s) reference broker name(s) {sorted(unknown)}, "
                f"but no BrokerConfig is registered under those name(s). "
                f"Registered names: {sorted(registered)}. "
                f"strict=True on at least one BrokerConfig promotes this to an "
                f"init-time error."
            )
            raise MarkerConfigurationError(msg)
        logger.warning(
            "FastStream marker(s) reference broker name(s) %s, but no "
            "BrokerConfig is registered under those name(s). Registered "
            "names: %s. The markers will be silently ignored. "
            "Pass strict=True to BrokerConfig to fail fast on unknown names.",
            sorted(unknown),
            sorted(registered),
        )

    # ----- combined AsyncAPI ---------------------------------------------

    def _build_combined_asyncapi(self) -> Any:
        if self._combined_schema is None:
            self._combined_schema = AsyncAPI(
                *[child.broker for child in self._children],
                title=self.config.title or "FastStream",
                version=self.config.version or "0.1.0",
                description=self.config.description,
                tags=tuple(self.config.tags),
            )
        return build_asyncapi_controller(
            schema=self._combined_schema,
            asyncapi_url=self.config.asyncapi_url,
            include_in_schema=self.config.asyncapi_include_in_schema,
        )
