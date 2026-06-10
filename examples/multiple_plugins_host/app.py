"""Multi-broker demo: Kafka (durable log) + Redis (fan-out).

The single :class:`FastStreamPlugin` hosts both brokers and serves a combined
AsyncAPI document at ``/asyncapi``.
"""

from dataclasses import dataclass

from faststream import Logger
from faststream.kafka import KafkaBroker
from faststream.redis import RedisBroker
from litestar import Controller, Litestar, post

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    subscriber,
)


@dataclass
class Order:
    user_id: int
    item: str


kafka_broker = KafkaBroker("localhost:9092")
redis_broker = RedisBroker("redis://localhost:6379")


class OrdersController(Controller):
    path = "/orders"

    @post("/")
    async def create_order(self, data: Order, kafka: KafkaBroker) -> dict:
        # Inject the kafka broker by ``BrokerConfig.name`` — the redis broker
        # is also registered (under ``redis``) but unused on this route.
        await kafka.publish(data, "orders.new")
        return {"queued": True}

    @staticmethod
    @subscriber("orders.new", plugin="kafka")
    async def fanout_notification(payload: Order, logger: Logger) -> None:
        notification = {
            "user_id": payload.user_id,
            "item": payload.item,
            "status": "received",
        }
        logger.info("forwarding to redis", extra=notification)
        await redis_broker.publish(notification, "orders.notify")


plugin = FastStreamPlugin(
    config=FastStreamConfig(
        brokers=[
            BrokerConfig(broker=kafka_broker, name="kafka"),
            BrokerConfig(broker=redis_broker, name="redis"),
        ],
        asyncapi_url="/asyncapi",
        title="Orders App",
        description="Kafka durable log + Redis fan-out",
    ),
)


@plugin.after_startup("kafka")
async def announce_kafka(app: Litestar) -> None:
    app.logger.info("kafka broker ready")


@plugin.after_startup("redis")
async def announce_redis(app: Litestar) -> None:
    app.logger.info("redis broker ready")


app = Litestar(
    plugins=[plugin],
    route_handlers=[OrdersController],
)
