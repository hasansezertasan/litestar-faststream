"""Scenario B: same codebase, two processes — API publishes, worker consumes.

How to run:

* API pod (HTTP + publish-only broker): ``uvicorn examples.publish_only.app:app``
* Worker pod (full broker, no HTTP):    ``litestar --app examples.publish_only.app:app faststream run``

The ``publish_only=True`` flag on a ``BrokerConfig`` tells the plugin to call
``broker.connect()`` but skip ``broker.start()`` in the ASGI lifespan.
``broker.publish(...)`` from the HTTP handler still works because it only
needs an open connection. The HTTP handler injects the broker via Litestar DI
(``rabbit: NamedDependency[RabbitBroker]``) and calls ``rabbit.publish(...)`` directly. The
``@subscriber`` below is registered (and shows up in AsyncAPI) but won't fire
in the API process — the worker process, launched via ``litestar faststream
run``, ignores ``publish_only`` and starts the consume-loops.
"""

import os
from dataclasses import dataclass

from faststream import Logger
from faststream.rabbit import RabbitBroker
from litestar import Controller, Litestar, post
from litestar.di import NamedDependency

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    subscriber,
)

PUBLISH_ONLY = os.getenv("FASTSTREAM_PUBLISH_ONLY", "1") == "1"

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


@dataclass
class Order:
    user_id: int
    item: str


class OrdersController(Controller):
    path = "/orders"

    @post("/")
    async def create_order(
        self,
        data: Order,
        rabbit: NamedDependency[RabbitBroker],
    ) -> dict:
        await rabbit.publish(data, "orders.new")
        return {"queued": True, "user_id": data.user_id}


@subscriber("orders.new")
async def on_order(payload: Order, logger: Logger) -> None:
    logger.info("processing order user_id=%s item=%s", payload.user_id, payload.item)


# In production you wouldn't toggle this with an env var — the API pod's image
# would set ``publish_only=True`` and the worker pod would launch via
# ``litestar faststream run`` (which always runs the full lifecycle). The env var
# here is just so the same file demonstrates both shapes.
plugin = FastStreamPlugin(
    FastStreamConfig(
        brokers=[
            BrokerConfig(broker=broker, publish_only=PUBLISH_ONLY, handlers=[on_order]),
        ],
    ),
)

app = Litestar(plugins=[plugin], route_handlers=[OrdersController])
