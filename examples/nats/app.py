"""Full demo: HTTP routes, Controller stream handler, HTTP-to-broker via DI.

Also demonstrates dependency injection on both sides of the plugin:

* **FastStream subscribers** receive broker-scoped values via the typed aliases
  in ``faststream.nats.annotations`` — ``NatsMessage`` for the raw incoming
  message (core trio) and ``Client`` for the raw ``nats.aio.client.Client``
  (NATS-specific extra). ``JsClient``/``ObjectStorage``/``NatsKvMessage`` are
  also available when using JetStream / KV / object-store subscribers.
* **Litestar routes** receive the broker as a normal Litestar dependency keyed
  by the ``BrokerConfig`` name (defaults to ``"nats"``).
"""

from dataclasses import dataclass

from faststream import Logger
from faststream.nats import NatsBroker
from faststream.nats.annotations import Client, NatsMessage
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


broker = NatsBroker("nats://localhost:4222")


class OrdersController(Controller):
    path = "/orders"

    @get("/")
    async def list_orders(self) -> list[dict]:
        return []

    @post("/")
    async def create_order(
        self,
        data: Order,
        nats: NamedDependency[NatsBroker],
    ) -> dict:
        await nats.publish(data, "orders.new")
        return {"queued": True}

    @get("/stats")
    async def order_stats(self, nats: NamedDependency[NatsBroker]) -> dict:
        # Litestar DI: param name ``nats`` matches BrokerConfig.name; the
        # ``NatsBroker`` annotation resolves via signature_namespace.
        return {"broker": type(nats).__name__, "connected": True}

    @staticmethod
    @subscriber("orders.new")
    @publisher("orders.processed")
    async def on_order(
        payload: Order,
        message: NatsMessage,
        logger: Logger,
    ) -> dict:
        logger.info(
            "processing",
            extra={"user_id": payload.user_id, "msg_id": message.message_id},
        )
        return {"user_id": payload.user_id, "item": payload.item, "ok": True}

    @staticmethod
    @subscriber("orders.processed")
    async def on_processed(payload: dict, client: Client, logger: Logger) -> None:
        # ``Client`` is the raw nats.aio Client — use for ops FastStream does
        # not wrap (custom request/reply, KV management, etc.).
        logger.info(
            "acknowledged",
            extra={"user_id": payload["user_id"], "client": type(client).__name__},
        )


plugin = FastStreamPlugin(
    FastStreamConfig(
        brokers=[BrokerConfig(broker=broker)],
        asyncapi_url="/asyncapi",
    ),
)


@plugin.after_startup("nats")
async def announce(app: Litestar) -> None:
    app.logger.info("nats broker ready")


app = Litestar(
    plugins=[plugin],
    route_handlers=[OrdersController],
)
