"""Integration tests for Redis + Litestar."""

import pytest
from faststream.redis import RedisBroker, TestRedisBroker
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

REDIS_URL = "redis://localhost:6379"


@pytest.mark.redis()
class TestRedisLitestar(LitestarTestcase):
    broker_class = RedisBroker
    plugin_class = BrokerConfig
    test_broker_cm = TestRedisBroker
    broker_url = REDIS_URL
    destination_kwarg = "channel"


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_http_handler_receives_broker_via_di() -> None:
    broker = RedisBroker(REDIS_URL)

    @subscriber("di-target-redis")
    async def consumer(payload: dict) -> None: ...

    @get("/trigger")
    async def trigger(redis: NamedDependency[RedisBroker]) -> dict:
        await redis.publish({"hello": "world"}, channel="di-target-redis")
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
    async with TestRedisBroker(broker), AsyncTestClient(app) as client:
        resp = await client.get("/trigger")
        assert resp.status_code == 200
        consumer.mock.assert_called_once_with({"hello": "world"})


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_after_startup_hook_runs_after_broker_start() -> None:
    broker = RedisBroker(REDIS_URL)
    cfg = BrokerConfig(broker=broker)
    order: list[str] = []

    @cfg.after_startup
    async def hook(app: object) -> None:
        order.append("after")

    app = Litestar(plugins=[FastStreamPlugin(FastStreamConfig(brokers=[cfg]))])
    async with TestRedisBroker(broker), AsyncTestClient(app):
        pass
    assert order == ["after"]
