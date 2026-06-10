"""Integration tests for Kafka (aiokafka) + Litestar."""

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker
from litestar import Litestar, get
from litestar.testing import AsyncTestClient

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    subscriber,
)
from tests.integration.test_base import LitestarTestcase

KAFKA_URL = "localhost:9092"


@pytest.mark.kafka()
class TestKafkaLitestar(LitestarTestcase):
    broker_class = KafkaBroker
    plugin_class = BrokerConfig
    test_broker_cm = TestKafkaBroker
    broker_url = KAFKA_URL
    destination_kwarg = "topic"


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_http_handler_receives_broker_via_di() -> None:
    broker = KafkaBroker(KAFKA_URL)

    @subscriber("di-target-kafka")
    async def consumer(payload: dict) -> None: ...

    @get("/trigger")
    async def trigger(kafka: KafkaBroker) -> dict:
        await kafka.publish({"hello": "world"}, topic="di-target-kafka")
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
    async with TestKafkaBroker(broker), AsyncTestClient(app) as client:
        resp = await client.get("/trigger")
        assert resp.status_code == 200
        consumer.mock.assert_called_once_with({"hello": "world"})


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_after_startup_hook_runs_after_broker_start() -> None:
    broker = KafkaBroker(KAFKA_URL)
    cfg = BrokerConfig(broker=broker)
    order: list[str] = []

    @cfg.after_startup
    async def hook(app: object) -> None:
        order.append("after")

    app = Litestar(plugins=[FastStreamPlugin(FastStreamConfig(brokers=[cfg]))])
    async with TestKafkaBroker(broker), AsyncTestClient(app):
        pass
    assert order == ["after"]
