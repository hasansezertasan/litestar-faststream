"""Tests for FastStreamPlugin (multi-broker host plugin)."""

from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar import Litestar
from litestar.exceptions import ImproperlyConfiguredException

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    publisher,
    subscriber,
)


def _fake_broker(class_name: str = "RabbitBroker") -> MagicMock:
    broker = MagicMock(name=f"broker-{class_name}")
    broker.connect = AsyncMock()
    broker.start = AsyncMock()
    broker.stop = AsyncMock()
    broker.close = AsyncMock()
    broker._subscribers = []
    broker.__class__.__name__ = class_name

    def subscriber_call(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        def deco(fn):  # noqa: ANN202
            return fn

        return deco

    broker.subscriber = MagicMock(side_effect=subscriber_call)
    broker.publisher = MagicMock(side_effect=lambda *a, **k: lambda fn: fn)
    # Tests that don't exercise per-broker AsyncAPI rendering use the override
    # to dodge faststream's real schema construction.
    broker._faststream_litestar_schema_override = MagicMock(
        to_jsonable=dict,
        to_json=lambda: "{}",
        to_yaml=lambda: "",
    )
    return broker


# ---------- construction validation -----------------------------------


def test_broker_spec_publish_only_propagates() -> None:
    host = FastStreamPlugin(
        config=FastStreamConfig(
            brokers=[
                BrokerConfig(broker=_fake_broker(), name="pub", publish_only=True),
                BrokerConfig(broker=_fake_broker("KafkaBroker"), name="full"),
            ],
        ),
    )
    children = {c.name: c for c in host.brokers}
    assert children["pub"].publish_only is True
    assert children["pub"]._composer.publish_only is True
    assert children["full"].publish_only is False
    assert children["full"]._composer.publish_only is False


def test_empty_brokers_rejected() -> None:
    with pytest.raises(ImproperlyConfiguredException, match="at least one broker"):
        FastStreamPlugin(config=FastStreamConfig(brokers=[]))


def test_duplicate_names_rejected() -> None:
    with pytest.raises(ImproperlyConfiguredException, match="Duplicate broker name"):
        FastStreamPlugin(
            config=FastStreamConfig(
                brokers=[
                    BrokerConfig(broker=_fake_broker(), name="x"),
                    BrokerConfig(broker=_fake_broker(), name="x"),
                ],
            ),
        )


def test_unknown_entry_type_rejected() -> None:
    with pytest.raises(
        ImproperlyConfiguredException,
        match="BrokerConfig instances",
    ):
        FastStreamPlugin(config=FastStreamConfig(brokers=cast("Any", [object()])))


# ---------- composition & delegation ----------------------------------


def test_accepts_pre_built_broker_plugins() -> None:
    child = BrokerConfig(broker=_fake_broker(), name="k")
    host = FastStreamPlugin(config=FastStreamConfig(brokers=[child]))
    assert host.brokers == (child,)


def test_each_child_registered_with_its_broker() -> None:
    @subscriber("orders.new", plugin="kafka")
    async def kafka_handler(payload: dict) -> None: ...

    @subscriber("notify.user", plugin="redis")
    async def redis_handler(payload: dict) -> None: ...

    kafka = _fake_broker("KafkaBroker")
    redis = _fake_broker("RedisBroker")
    host = FastStreamPlugin(
        config=FastStreamConfig(
            brokers=[
                BrokerConfig(
                    broker=kafka,
                    name="kafka",
                    handlers=[kafka_handler],
                ),
                BrokerConfig(
                    broker=redis,
                    name="redis",
                    handlers=[redis_handler],
                ),
            ],
        ),
    )
    Litestar(plugins=[host])

    kafka_targets = [c.args for c in kafka.subscriber.call_args_list]
    redis_targets = [c.args for c in redis.subscriber.call_args_list]
    assert ("orders.new",) in kafka_targets
    assert ("orders.new",) not in redis_targets
    assert ("notify.user",) in redis_targets
    assert ("notify.user",) not in kafka_targets


def test_after_startup_hook_routes_to_named_broker() -> None:
    host = FastStreamPlugin(
        config=FastStreamConfig(
            brokers=[
                BrokerConfig(broker=_fake_broker(), name="kafka"),
                BrokerConfig(broker=_fake_broker(), name="redis"),
            ],
        ),
    )

    @host.after_startup("kafka")
    async def hook(app) -> None: ...

    kafka_child = host._child("kafka")
    redis_child = host._child("redis")
    assert hook in kafka_child._composer._after_startup
    assert hook not in redis_child._composer._after_startup


def test_after_startup_unknown_name_raises() -> None:
    host = FastStreamPlugin(
        config=FastStreamConfig(
            brokers=[BrokerConfig(broker=_fake_broker(), name="kafka")],
        ),
    )
    with pytest.raises(ImproperlyConfiguredException, match="No broker named 'bogus'"):
        host.after_startup("bogus")


def test_on_broker_shutdown_hook_routes_to_named_broker() -> None:
    host = FastStreamPlugin(
        config=FastStreamConfig(
            brokers=[BrokerConfig(broker=_fake_broker(), name="kafka")],
        ),
    )

    @host.on_broker_shutdown("kafka")
    async def hook(app) -> None: ...

    assert hook in host._child("kafka")._composer._on_shutdown


def test_on_cli_init_fans_out_to_every_child() -> None:
    """Host plugin should forward CLI registration to every hosted broker."""
    kafka = _fake_broker("KafkaBroker")
    redis = _fake_broker("RedisBroker")
    host = FastStreamPlugin(
        config=FastStreamConfig(
            brokers=[
                BrokerConfig(broker=kafka, name="kafka"),
                BrokerConfig(broker=redis, name="redis"),
            ],
        ),
    )
    cli = MagicMock(name="cli")

    kafka_child = host._child("kafka")
    redis_child = host._child("redis")
    kafka_called: list[Any] = []
    redis_called: list[Any] = []
    kafka_child._apply_to_cli = cast("Any", kafka_called.append)
    redis_child._apply_to_cli = cast("Any", redis_called.append)

    host.on_cli_init(cli)

    assert kafka_called == [cli]
    assert redis_called == [cli]


def test_combined_asyncapi_route_mounted() -> None:
    """Plugin-level asyncapi_url should mount one AsyncAPI controller."""
    host = FastStreamPlugin(
        config=FastStreamConfig(
            brokers=[
                BrokerConfig(
                    broker=_fake_broker("KafkaBroker"),
                    name="kafka",
                ),
                BrokerConfig(
                    broker=_fake_broker("RedisBroker"),
                    name="redis",
                ),
            ],
            asyncapi_url="/asyncapi",
            title="My App",
        ),
    )
    app = Litestar(plugins=[host])
    paths = {r.path for r in app.routes}
    assert "/asyncapi" in paths
    assert "/asyncapi.json" in paths
    assert "/asyncapi.yaml" in paths


def test_publisher_on_http_handler_rejected() -> None:
    """@publisher on a Litestar HTTP route handler fails at app init."""
    from litestar import Litestar, post

    kafka = _fake_broker("KafkaBroker")

    @post("/orders")
    @publisher("orders.new", plugin="kafka")
    async def create_order(data: dict) -> dict:
        return {"ok": True}

    host = FastStreamPlugin(
        config=FastStreamConfig(brokers=[BrokerConfig(broker=kafka, name="kafka")]),
    )
    with pytest.raises(
        ImproperlyConfiguredException,
        match="cannot decorate an HTTP route handler",
    ):
        Litestar(plugins=[host], route_handlers=[create_order])
