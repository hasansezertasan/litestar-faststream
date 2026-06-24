"""Integration tests for NATS + Litestar."""

import pytest
from faststream.nats import NatsBroker, TestNatsBroker
from litestar import Litestar, get
from litestar.di import NamedDependency
from litestar.testing import AsyncTestClient

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    subscriber,
)
from tests.integration.test_base import LitestarTestcase

NATS_URL = "nats://localhost:4222"


@pytest.mark.nats()
class TestNatsLitestar(LitestarTestcase):
    broker_class = NatsBroker
    plugin_class = BrokerConfig
    test_broker_cm = TestNatsBroker
    broker_url = NATS_URL
    destination_kwarg = "subject"


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_http_handler_receives_broker_via_di() -> None:
    broker = NatsBroker(NATS_URL)

    @subscriber("di-target-nats")
    async def consumer(payload: dict) -> None: ...

    @get("/trigger")
    async def trigger(nats: NamedDependency[NatsBroker]) -> dict:
        await nats.publish({"hello": "world"}, subject="di-target-nats")
        return {"published": True}

    app = Litestar(
        plugins=[
            FastStreamPlugin(
                FastStreamConfig(
                    brokers=[BrokerConfig(broker=broker, handlers=[consumer])],
                ),
            ),
        ],
        route_handlers=[trigger],
    )
    async with TestNatsBroker(broker), AsyncTestClient(app) as client:
        resp = await client.get("/trigger")
        assert resp.status_code == 200
        consumer.mock.assert_called_once_with({"hello": "world"})


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_after_startup_hook_runs_after_broker_start() -> None:
    broker = NatsBroker(NATS_URL)
    cfg = BrokerConfig(broker=broker)
    order: list[str] = []

    @cfg.after_startup
    async def hook(app: object) -> None:
        order.append("after")

    app = Litestar(plugins=[FastStreamPlugin(FastStreamConfig(brokers=[cfg]))])
    async with TestNatsBroker(broker), AsyncTestClient(app):
        pass
    assert order == ["after"]
