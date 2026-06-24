"""Full demo: HTTP routes, Controller stream handler, HTTP-to-broker via DI.

Also demonstrates dependency injection on both sides of the plugin:

* **FastStream subscribers** receive broker-scoped values via the typed aliases
  in ``faststream.kafka.annotations`` — ``KafkaMessage`` for the raw incoming
  message (core trio), ``Consumer`` for the underlying ``AIOKafkaConsumer``
  (Kafka-specific extra), and ``KafkaProducer`` for the raw producer.
* **Litestar routes** receive the broker as a normal Litestar dependency keyed
  by the ``BrokerConfig`` name (defaults to ``"kafka"``); the broker class is
  pre-registered in ``signature_namespace``.
"""

from dataclasses import dataclass

from faststream import Logger
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import Consumer, KafkaMessage, KafkaProducer
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


broker = KafkaBroker("localhost:9092")


class OrdersController(Controller):
    path = "/orders"

    @get("/")
    async def list_orders(self) -> list[dict]:
        return []

    @post("/")
    async def create_order(
        self,
        data: Order,
        kafka: NamedDependency[KafkaBroker],
    ) -> dict:
        await kafka.publish(data, "orders.new")
        return {"queued": True}

    @get("/stats")
    async def order_stats(self, kafka: NamedDependency[KafkaBroker]) -> dict:
        # Litestar DI: param name ``kafka`` matches BrokerConfig.name; the
        # ``KafkaBroker`` annotation resolves via signature_namespace.
        return {"broker": type(kafka).__name__, "connected": True}

    @staticmethod
    @subscriber("orders.new")
    @publisher("orders.processed")
    async def on_order(
        payload: Order,
        message: KafkaMessage,
        consumer: Consumer,
        logger: Logger,
    ) -> dict:
        # ``Consumer`` is the per-handler AIOKafkaConsumer (extra). Useful for
        # introspecting offsets/partitions or calling ``commit()`` manually.
        logger.info(
            "processing",
            extra={
                "user_id": payload.user_id,
                "msg_id": message.message_id,
                "consumer": type(consumer).__name__,
            },
        )
        return {"user_id": payload.user_id, "item": payload.item, "ok": True}

    @staticmethod
    @subscriber("orders.processed")
    async def on_processed(
        payload: dict,
        producer: KafkaProducer,
        logger: Logger,
    ) -> None:
        # ``KafkaProducer`` is the raw AioKafkaFastProducer — reach for it when
        # you need to publish outside the @publisher decorator contract.
        logger.info(
            "acknowledged",
            extra={"user_id": payload["user_id"], "producer": type(producer).__name__},
        )


plugin = FastStreamPlugin(
    FastStreamConfig(
        brokers=[BrokerConfig(broker=broker)],
        asyncapi_url="/asyncapi",
    ),
)


@plugin.after_startup("kafka")
async def announce(app: Litestar) -> None:
    app.logger.info("kafka broker ready")


app = Litestar(
    plugins=[plugin],
    route_handlers=[OrdersController],
)
