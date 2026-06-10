"""Full demo: HTTP routes, Controller stream handler, HTTP-to-broker via DI.

Uses FastStream's Confluent backend (librdkafka via confluent-kafka-python).
The wire protocol is identical to the pure-Python Kafka backend, but the
broker class is imported from ``faststream.confluent``.

Also demonstrates dependency injection on both sides of the plugin:

* **FastStream subscribers** receive broker-scoped values via the typed aliases
  in ``faststream.confluent.annotations`` — ``KafkaMessage`` (core trio) and
  ``KafkaProducer`` (raw ``AsyncConfluentFastProducer``).
* **Litestar routes** receive the broker as a normal Litestar dependency keyed
  by the ``BrokerConfig`` name (default ``"kafka"`` — same as the pure-Python
  backend because the class is also called ``KafkaBroker``).
"""

from dataclasses import dataclass

from faststream import Logger
from faststream.confluent import KafkaBroker
from faststream.confluent.annotations import KafkaMessage, KafkaProducer
from litestar import Controller, Litestar, get, post

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


broker = KafkaBroker("localhost:9092")


class OrdersController(Controller):
    path = "/orders"

    @get("/")
    async def list_orders(self) -> list[dict]:
        return []

    @post("/")
    async def create_order(self, data: Order, kafka: KafkaBroker) -> dict:
        await kafka.publish(data, "orders.new")
        return {"queued": True}

    @get("/stats")
    async def order_stats(self, kafka: KafkaBroker) -> dict:
        # Litestar DI: param name ``kafka`` matches BrokerConfig.name; the
        # confluent ``KafkaBroker`` resolves via signature_namespace.
        return {"broker": type(kafka).__name__, "connected": True}

    @staticmethod
    @subscriber("orders.new")
    @publisher("orders.processed")
    async def on_order(
        payload: Order,
        message: KafkaMessage,
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
        producer: KafkaProducer,
        logger: Logger,
    ) -> None:
        # ``KafkaProducer`` is the raw AsyncConfluentFastProducer — useful for
        # publishing outside the @publisher decorator (admin events, DLQs, ...).
        logger.info(
            "acknowledged",
            extra={"user_id": payload["user_id"], "producer": type(producer).__name__},
        )


plugin = FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))


@plugin.after_startup("confluent")
async def announce(app: Litestar) -> None:
    app.logger.info("confluent broker ready")


app = Litestar(
    plugins=[plugin],
    route_handlers=[OrdersController],
)
