"""Tests for ``FastStreamConfig.enable_otel`` tri-state resolution.

Covers:
* fail-fast at ``on_app_init`` when ``enable_otel=True`` and OTel missing
* ``enable_otel=False`` skips even when OTel is present
* auto-detect (``None``) flips on iff Litestar has ``OpenTelemetryPlugin``
* idempotency: injecting twice doesn't double-wrap
* ``_has_otel_plugin`` honours subclasses via the typed registry
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from litestar.config.app import AppConfig

from litestar_faststream import (
    BrokerConfig,
    BrokerConfigurationError,
    FastStreamConfig,
    FastStreamPlugin,
)
from litestar_faststream import host as host_mod
from litestar_faststream.host import _has_otel_plugin


def _fake_broker() -> MagicMock:
    broker = MagicMock()
    broker.middlewares = []
    broker.add_middleware = MagicMock(
        side_effect=broker.middlewares.append,
    )
    return broker


def _plugin(*, enable_otel: bool | None, brokers: int = 1) -> FastStreamPlugin:
    return FastStreamPlugin(
        FastStreamConfig(
            brokers=[
                BrokerConfig(broker=_fake_broker(), name=f"b{i}")
                for i in range(brokers)
            ],
            enable_otel=enable_otel,
        ),
    )


# ----- fail-fast at on_app_init -------------------------------------------


def test_enable_otel_true_without_otel_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(host_mod, "BrokerConfigurationError", BrokerConfigurationError)
    # Patch the flag the plugin reads to simulate the missing-dep environment.
    from litestar_faststream import _otel_typing

    monkeypatch.setattr(_otel_typing, "OPENTELEMETRY_INSTALLED", False)
    plugin = _plugin(enable_otel=True)
    with pytest.raises(BrokerConfigurationError, match="not installed"):
        plugin.on_app_init(AppConfig())


def test_enable_otel_true_with_otel_does_not_raise() -> None:
    """OTel is installed in the test env; explicit True should succeed."""
    pytest.importorskip("opentelemetry")
    plugin = _plugin(enable_otel=True)
    plugin.on_app_init(AppConfig())  # no exception


# ----- explicit False bypass ---------------------------------------------


def test_enable_otel_false_skips_resolution() -> None:
    plugin = _plugin(enable_otel=False)
    plugin.on_app_init(AppConfig())
    # The pre-startup hook should NOT have been added; nothing to assert
    # beyond no crash. Below we check the middleware list stays empty.
    for child in plugin._children:
        assert child.broker.middlewares == []


# ----- auto-detect at startup --------------------------------------------


@pytest.mark.asyncio()
async def test_auto_detect_enables_when_litestar_otel_plugin_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("opentelemetry")

    # Stub _has_otel_plugin to short-circuit the real registry check.
    monkeypatch.setattr(host_mod, "_has_otel_plugin", lambda app: True)
    plugin = _plugin(enable_otel=None, brokers=2)
    plugin.on_app_init(AppConfig())

    fake_app = SimpleNamespace()
    await plugin._install_otel_if_enabled(fake_app)

    from litestar_faststream.instrumentation import OtelMiddleware

    for child in plugin._children:
        assert OtelMiddleware in child.broker.middlewares


@pytest.mark.asyncio()
async def test_auto_detect_skips_when_no_litestar_otel_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(host_mod, "_has_otel_plugin", lambda app: False)
    plugin = _plugin(enable_otel=None)
    plugin.on_app_init(AppConfig())
    await plugin._install_otel_if_enabled(SimpleNamespace())
    for child in plugin._children:
        assert child.broker.middlewares == []


# ----- idempotency --------------------------------------------------------


@pytest.mark.asyncio()
async def test_double_install_does_not_double_wrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("opentelemetry")
    monkeypatch.setattr(host_mod, "_has_otel_plugin", lambda app: True)
    plugin = _plugin(enable_otel=None)
    plugin.on_app_init(AppConfig())
    await plugin._install_otel_if_enabled(SimpleNamespace())
    await plugin._install_otel_if_enabled(SimpleNamespace())

    from litestar_faststream.instrumentation import OtelMiddleware

    for child in plugin._children:
        assert child.broker.middlewares.count(OtelMiddleware) == 1


@pytest.mark.asyncio()
async def test_skip_when_user_already_added_otel_middleware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """User-attached middleware takes precedence; auto-inject is a no-op."""
    pytest.importorskip("opentelemetry")
    monkeypatch.setattr(host_mod, "_has_otel_plugin", lambda app: True)

    from litestar_faststream.instrumentation import OtelMiddleware

    user_broker = _fake_broker()
    user_broker.middlewares = [OtelMiddleware]
    plugin = FastStreamPlugin(
        FastStreamConfig(
            brokers=[BrokerConfig(broker=user_broker, name="rabbit")],
            enable_otel=None,
        ),
    )
    plugin.on_app_init(AppConfig())
    await plugin._install_otel_if_enabled(SimpleNamespace())
    # add_middleware should NOT have been called.
    user_broker.add_middleware.assert_not_called()


# ----- _has_otel_plugin ----------------------------------------------------


def test_has_otel_plugin_returns_false_when_import_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The submodule may not exist (litestar[opentelemetry] not installed)."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "litestar.plugins.opentelemetry":
            msg = "simulated"
            raise ImportError(msg)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert _has_otel_plugin(SimpleNamespace()) is False


def test_has_otel_plugin_uses_typed_registry() -> None:
    """``app.plugins.get(OpenTelemetryPlugin)`` is the contract -- not iteration."""
    pytest.importorskip("litestar.plugins.opentelemetry")
    from litestar.plugins.opentelemetry import OpenTelemetryPlugin

    fake_plugin = MagicMock(spec=OpenTelemetryPlugin)
    app = SimpleNamespace(plugins=SimpleNamespace(get=lambda cls: fake_plugin))
    assert _has_otel_plugin(app) is True

    app_empty = SimpleNamespace(plugins=SimpleNamespace(get=lambda cls: None))
    assert _has_otel_plugin(app_empty) is False
