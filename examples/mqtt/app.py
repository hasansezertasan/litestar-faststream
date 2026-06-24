"""Full demo: HTTP routes, Controller stream handler, HTTP-to-broker via DI.

Also demonstrates dependency injection on both sides of the plugin:

* **FastStream subscribers** receive broker-scoped values via the typed aliases
  in ``faststream.mqtt.annotations`` — ``MQTTMessage`` for the raw incoming
  message (core trio). Note: unlike the other brokers, FastStream does not
  ship a raw-client annotation for MQTT; reach for ``broker._connection``
  manually if you need the underlying ``zmqtt.MQTTClient`` (good candidate
  for upstreaming).
* **Litestar routes** receive the broker as a normal Litestar dependency keyed
  by the ``BrokerConfig`` name (defaults to ``"mqtt"``).
"""

from dataclasses import dataclass

from faststream import Logger
from faststream.mqtt import MQTTBroker
from faststream.mqtt.annotations import MQTTMessage
from litestar import Controller, Litestar, get, post
from litestar.di import NamedDependency

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    publisher,
    subscriber,
)


@dataclass
class Order:
    user_id: int
    item: str


broker = MQTTBroker("localhost")


class OrdersController(Controller):
    path = "/orders"

    @get("/")
    async def list_orders(self) -> list[dict]:
        return []

    @post("/")
    async def create_order(
        self,
        data: Order,
        mqtt: NamedDependency[MQTTBroker],
    ) -> dict:
        await mqtt.publish(data, "orders/new")
        return {"queued": True}

    @get("/stats")
    async def order_stats(self, mqtt: NamedDependency[MQTTBroker]) -> dict:
        # Litestar DI: param name ``mqtt`` matches BrokerConfig.name; the
        # ``MQTTBroker`` annotation resolves via signature_namespace.
        return {"broker": type(mqtt).__name__, "connected": True}

    @staticmethod
    @subscriber("orders/new")
    @publisher("orders/processed")
    async def on_order(
        payload: Order,
        message: MQTTMessage,
        logger: Logger,
    ) -> dict:
        logger.info(
            "processing",
            extra={"user_id": payload.user_id, "msg_id": message.message_id},
        )
        return {"user_id": payload.user_id, "item": payload.item, "ok": True}

    @staticmethod
    @subscriber("orders/processed")
    async def on_processed(payload: dict, message: MQTTMessage, logger: Logger) -> None:
        # No raw-client annotation upstream for MQTT yet — mirror the other
        # examples by injecting the raw message and logging msg metadata.
        logger.info(
            "acknowledged",
            extra={"user_id": payload["user_id"], "msg_id": message.message_id},
        )


plugin = FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))


@plugin.after_startup("mqtt")
async def announce(app: Litestar) -> None:
    app.logger.info("mqtt broker ready")


app = Litestar(
    plugins=[plugin],
    route_handlers=[OrdersController],
)
