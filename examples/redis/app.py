"""Full demo: HTTP routes, Controller stream handler, HTTP-to-broker via DI.

Also demonstrates dependency injection on both sides of the plugin:

* **FastStream subscribers** receive broker-scoped values via the typed aliases
  in ``faststream.redis.annotations`` — ``RedisChannelMessage`` for the raw
  incoming message (core trio), ``Redis`` for the raw client (core trio), and
  ``Pipeline`` for atomic ops (Redis-specific extra).
* **Litestar routes** receive the broker as a normal Litestar dependency keyed
  by the ``BrokerConfig`` name (defaults to ``"redis"``); the broker class is
  pre-registered in ``signature_namespace`` so handlers can type-hint it without
  importing it locally.
"""

from dataclasses import dataclass

from faststream import Logger
from faststream.redis import RedisBroker
from faststream.redis.annotations import Pipeline, Redis, RedisChannelMessage
from litestar import Controller, Litestar, get, post

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    publisher,
    subscriber,
)

PROCESSED_COUNTER = "orders:processed"
PROCESSED_LOG = "orders:log"


@dataclass
class Order:
    user_id: int
    item: str


broker = RedisBroker("redis://localhost:6379")


class OrdersController(Controller):
    path = "/orders"

    @get("/")
    async def list_orders(self) -> list[dict]:
        return []

    @post("/")
    async def create_order(self, data: Order, redis: RedisBroker) -> dict:
        await redis.publish(data, "orders.new")
        return {"queued": True}

    @get("/stats")
    async def order_stats(self, redis: RedisBroker) -> dict:
        # Litestar DI: param name ``redis`` matches BrokerConfig.name; type hint
        # resolves via signature_namespace (no extra imports needed in user
        # modules). Drop to the raw client to read the counter maintained below.
        count = await redis._connection.get(PROCESSED_COUNTER)
        return {"processed": int(count or 0)}

    @staticmethod
    @subscriber("orders.new")
    @publisher("orders.processed")
    async def on_order(
        payload: Order,
        message: RedisChannelMessage,
        pipe: Pipeline,
        logger: Logger,
    ) -> dict:
        # ``Pipeline`` is provided by ``Depends(get_pipe)`` upstream, which does
        # ``async with redis.pipeline() as pipe: yield pipe``. The context-manager
        # exit only RESETS the pipeline — it does NOT auto-execute — so you must
        # call ``pipe.execute()`` yourself to flush buffered commands atomically.
        # In redis-py async, ``pipe.incr/lpush`` buffer (return self, not awaited);
        # only ``pipe.execute()`` is awaitable.
        logger.info(
            "processing",
            extra={"user_id": payload.user_id, "msg_id": message.message_id},
        )
        pipe.incr(PROCESSED_COUNTER)
        pipe.lpush(PROCESSED_LOG, f"{payload.user_id}:{payload.item}")
        await pipe.execute()
        return {"user_id": payload.user_id, "item": payload.item, "ok": True}

    @staticmethod
    @subscriber("orders.processed")
    async def on_processed(payload: dict, redis: Redis, logger: Logger) -> None:
        # ``Redis`` is the raw ``redis.asyncio.Redis`` client. Against a live
        # broker you would call things like
        # ``await redis.set(f"orders:last:{payload['user_id']}", payload["item"], ex=300)``;
        # we only log here so the example stays runnable under TestRedisBroker
        # (which mocks ``broker._connection`` as a non-async MagicMock).
        logger.info(
            "acknowledged",
            extra={"user_id": payload["user_id"], "client": type(redis).__name__},
        )


plugin = FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))


@plugin.after_startup("redis")
async def announce(app: Litestar) -> None:
    app.logger.info("redis broker ready")


app = Litestar(
    plugins=[plugin],
    route_handlers=[OrdersController],
)
