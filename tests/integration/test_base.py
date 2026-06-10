"""Shared Litestar-integration test cases parameterized for any broker."""

from typing import Any, TypeVar

import pytest
from faststream._internal.broker import BrokerUsecase
from litestar import Litestar
from litestar.testing import AsyncTestClient

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    subscriber,
)

Broker = TypeVar("Broker", bound=BrokerUsecase)


class LitestarTestcase:
    """Shared test cases for any FastStream broker's Litestar integration.

    Subclasses MUST define:
      * `broker_class` -- concrete broker class (e.g. RabbitBroker)
      * `plugin_class` -- concrete BrokerConfig subclass (e.g. BrokerConfig)
      * `test_broker_cm` -- async-context-manager class for in-memory broker
                            (e.g. TestRabbitBroker)
      * `destination_kwarg` -- the name FastStream's per-broker ``publish``
                               uses for the destination (``queue`` for
                               rabbit, ``topic`` for kafka/mqtt, ``subject``
                               for nats, ``channel`` for redis).
    """

    broker_class: type[Any]
    plugin_class: type[BrokerConfig]
    test_broker_cm: type[Any]
    broker_url: str = "amqp://guest:guest@localhost:5672/"
    destination_kwarg: str = "queue"

    def make_broker(self) -> Any:
        return self.broker_class(self.broker_url)

    @pytest.mark.asyncio()
    async def test_subscriber_via_handlers_arg(self) -> None:
        broker = self.make_broker()

        @subscriber("ping")
        async def on_ping(name: str) -> str:
            return f"hello {name}"

        app = Litestar(
            plugins=[
                FastStreamPlugin(
                    FastStreamConfig(
                        brokers=[
                            self.plugin_class(broker=broker, handlers=[on_ping]),
                        ],
                    ),
                ),
            ],
        )

        async with self.test_broker_cm(broker), AsyncTestClient(app):
            await broker.publish("Alice", **{self.destination_kwarg: "ping"})
            on_ping.mock.assert_called_once_with("Alice")
