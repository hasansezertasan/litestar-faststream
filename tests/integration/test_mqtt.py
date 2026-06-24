"""Integration tests for MQTT + Litestar."""

import pytest

# zmqtt (third-party transport required by faststream.mqtt) is only
# available on Python >= 3.11; skip the entire module elsewhere so test
# collection doesn't fail on older interpreters.
pytest.importorskip("zmqtt")

from faststream.mqtt import MQTTBroker, TestMQTTBroker
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

MQTT_HOST = "localhost"


@pytest.mark.mqtt()
class TestMQTTLitestar(LitestarTestcase):
    broker_class = MQTTBroker
    plugin_class = BrokerConfig
    test_broker_cm = TestMQTTBroker
    broker_url = MQTT_HOST
    destination_kwarg = "topic"


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_http_handler_receives_broker_via_di() -> None:
    """BrokerConfig default name is 'mqtt' (broker class 'MQTTBroker' -> 'mqtt')."""
    broker = MQTTBroker(MQTT_HOST)

    @subscriber("di-target-mqtt")
    async def consumer(payload: dict) -> None: ...

    @get("/trigger")
    async def trigger(mqtt: NamedDependency[MQTTBroker]) -> dict:
        await mqtt.publish({"hello": "world"}, topic="di-target-mqtt")
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
    async with TestMQTTBroker(broker), AsyncTestClient(app) as client:
        resp = await client.get("/trigger")
        assert resp.status_code == 200
        consumer.mock.assert_called_once_with({"hello": "world"})


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_after_startup_hook_runs_after_broker_start() -> None:
    broker = MQTTBroker(MQTT_HOST)
    cfg = BrokerConfig(broker=broker)
    order: list[str] = []

    @cfg.after_startup
    async def hook(app: object) -> None:
        order.append("after")

    app = Litestar(plugins=[FastStreamPlugin(FastStreamConfig(brokers=[cfg]))])
    async with TestMQTTBroker(broker), AsyncTestClient(app):
        pass
    assert order == ["after"]
