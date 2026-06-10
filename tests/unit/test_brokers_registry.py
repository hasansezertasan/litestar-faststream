"""Tests for the ``Brokers`` registry + ``brokers`` DI dependency."""

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest
from litestar.config.app import AppConfig

from litestar_faststream import (
    BrokerConfig,
    BrokerNotRegisteredError,
    Brokers,
    FastStreamConfig,
    FastStreamPlugin,
)


def _fake_broker(name: str) -> MagicMock:
    broker = MagicMock(name=f"broker-{name}")
    broker.__class__.__name__ = f"{name.capitalize()}Broker"
    return broker


# ----- Brokers (standalone) -------------------------------------------------


def test_brokers_get_returns_registered_broker() -> None:
    b = _fake_broker("rabbit")
    reg = Brokers({"rabbit": b})
    assert reg.get("rabbit") is b
    assert reg["rabbit"] is b
    assert "rabbit" in reg


def test_brokers_get_missing_raises_typed_error() -> None:
    reg = Brokers({"rabbit": _fake_broker("rabbit")})
    with pytest.raises(BrokerNotRegisteredError) as exc_info:
        reg.get("redis")
    # Error must surface the registered names so typos are self-diagnosing.
    assert "rabbit" in str(exc_info.value)
    assert "redis" in str(exc_info.value)


def test_brokers_get_with_default_does_not_raise() -> None:
    reg = Brokers({})
    sentinel = object()
    assert reg.get("missing", sentinel) is sentinel
    assert reg.get("missing", None) is None


def test_brokers_is_a_mapping() -> None:
    """Standard Mapping operations work without surface-specific code."""
    rabbit, redis = _fake_broker("rabbit"), _fake_broker("redis")
    reg = Brokers({"rabbit": rabbit, "redis": redis})
    assert len(reg) == 2
    assert sorted(reg.keys()) == ["rabbit", "redis"]
    assert dict(reg.items()) == {"rabbit": rabbit, "redis": redis}
    assert sorted(reg) == ["rabbit", "redis"]


def test_brokers_is_immutable() -> None:
    """Mutating the constructor argument cannot affect the registry."""
    source: dict[str, Any] = {"rabbit": _fake_broker("rabbit")}
    reg = Brokers(source)
    source["redis"] = _fake_broker("redis")
    assert "redis" not in reg


def test_brokers_repr_shows_sorted_names() -> None:
    reg = Brokers({"redis": _fake_broker("redis"), "rabbit": _fake_broker("rabbit")})
    assert repr(reg) == "Brokers(['rabbit', 'redis'])"


# ----- Plugin wiring --------------------------------------------------------


def test_plugin_registers_brokers_dependency() -> None:
    rabbit = _fake_broker("rabbit")
    redis = _fake_broker("redis")
    plugin = FastStreamPlugin(
        FastStreamConfig(
            brokers=[
                BrokerConfig(broker=rabbit, name="rabbit"),
                BrokerConfig(broker=redis, name="redis"),
            ],
        ),
    )
    app_config = plugin.on_app_init(AppConfig())

    deps = app_config.dependencies or {}
    assert "brokers" in deps
    # Per-broker dependencies are kept alongside the aggregate registry.
    assert "rabbit" in deps
    assert "redis" in deps

    from typing import cast as _cast

    provide = deps["brokers"]
    registry = _cast("Any", provide).dependency()
    assert isinstance(registry, Brokers)
    assert registry.get("rabbit") is rabbit
    assert registry.get("redis") is redis


def test_plugin_does_not_override_user_brokers_dependency(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If a user already owns the ``brokers`` key, warn and skip."""
    user_dep = MagicMock(name="user-brokers")
    rabbit = _fake_broker("rabbit")
    plugin = FastStreamPlugin(
        FastStreamConfig(brokers=[BrokerConfig(broker=rabbit, name="rabbit")]),
    )
    seed = AppConfig()
    seed.dependencies = {"brokers": user_dep}
    with caplog.at_level(logging.WARNING, logger="litestar_faststream.host"):
        app_config = plugin.on_app_init(seed)

    assert app_config.dependencies["brokers"] is user_dep
    assert any("already registered" in r.message for r in caplog.records)


def test_plugin_exposes_brokers_in_signature_namespace() -> None:
    """Handlers can annotate ``brokers: Brokers`` without an explicit import."""
    plugin = FastStreamPlugin(
        FastStreamConfig(
            brokers=[BrokerConfig(broker=_fake_broker("rabbit"), name="rabbit")],
        ),
    )
    app_config = plugin.on_app_init(AppConfig())
    assert app_config.signature_namespace["Brokers"] is Brokers
