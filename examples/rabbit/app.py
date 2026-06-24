"""Full demo: HTTP routes, Controller stream handler, HTTP-to-broker via DI.

Also demonstrates dependency injection on both sides of the plugin:

* **FastStream subscribers** receive broker-scoped values via the typed aliases
  in ``faststream.rabbit.annotations`` — ``RabbitMessage`` for the raw incoming
  message (core trio), ``Channel`` / ``Connection`` for the underlying aio-pika
  ``RobustChannel`` and ``RobustConnection`` (Rabbit-specific extras), and
  ``RabbitProducer`` for the raw producer.
* **Litestar routes** receive the broker as a normal Litestar dependency keyed
  by the ``BrokerConfig`` name (defaults to ``"rabbit"``). The ``POST /orders``
  handler injects the broker and calls ``rabbit.publish(...)`` directly — this
  is the supported way to bridge HTTP requests onto a broker queue.
"""

from dataclasses import dataclass

from faststream import Logger
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import Channel, Connection, RabbitMessage
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


broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


class OrdersController(Controller):
    path = "/orders"

    @get("/")
    async def list_orders(self) -> list[dict]:
        return []

    @post("/")
    async def create_order(
        self,
        data: Order,
        rabbit: NamedDependency[RabbitBroker],
    ) -> dict:
        await rabbit.publish(data, "orders.new")
        return {"queued": True}

    @get("/stats")
    async def order_stats(self, rabbit: NamedDependency[RabbitBroker]) -> dict:
        # Litestar DI: param name ``rabbit`` matches BrokerConfig.name; the
        # ``RabbitBroker`` annotation resolves via signature_namespace.
        return {"broker": type(rabbit).__name__, "connected": True}

    @staticmethod
    @subscriber("orders.new")
    @publisher("orders.processed")
    async def on_order(
        payload: Order,
        message: RabbitMessage,
        logger: Logger,
    ) -> dict:
        logger.info(
            "processing",
            extra={"user_id": payload.user_id, "msg_id": message.message_id},
        )
        return {"user_id": payload.user_id, "item": payload.item, "ok": True}

    @staticmethod
    @subscriber("orders.processed")
    async def on_processed(
        payload: dict,
        channel: Channel,
        connection: Connection,
        logger: Logger,
    ) -> None:
        # ``Channel`` / ``Connection`` expose aio-pika's RobustChannel and
        # RobustConnection — use for ops FastStream does not wrap (declaring
        # exchanges manually, inspecting connection state, etc.).
        logger.info(
            "acknowledged",
            extra={
                "user_id": payload["user_id"],
                "channel": type(channel).__name__,
                "connection": type(connection).__name__,
            },
        )


plugin = FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))


@plugin.after_startup("rabbit")
async def announce(app: Litestar) -> None:
    app.logger.info("rabbit broker ready")


app = Litestar(
    plugins=[plugin],
    route_handlers=[OrdersController],
)
