"""Shared AsyncAPI HTTP-endpoint tests parameterized for any broker."""

import json
from typing import Any

import pytest
from litestar import Litestar
from litestar.testing import AsyncTestClient

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    subscriber,
)


class LitestarAsyncAPITestcase:
    """Shared AsyncAPI HTTP-endpoint tests for any FastStream broker.

    Subclasses MUST define:
      * ``broker_class`` -- concrete broker class (e.g. ``RabbitBroker``)
      * ``test_broker_cm`` -- in-memory broker async-context-manager
        (e.g. ``TestRabbitBroker``)
      * ``broker_url`` -- broker URL passed to ``broker_class``
    """

    broker_class: type[Any]
    test_broker_cm: type[Any]
    broker_url: str

    def _host(self, broker: Any, handler: Any) -> FastStreamPlugin:
        return FastStreamPlugin(
            FastStreamConfig(
                brokers=[BrokerConfig(broker=broker, handlers=[handler])],
                asyncapi_url="/asyncapi",
            ),
        )

    @pytest.mark.asyncio()
    async def test_asyncapi_html_endpoint_serves_html(self) -> None:
        broker = self.broker_class(self.broker_url)

        @subscriber("queue-1")
        async def h(payload: dict) -> None: ...

        app = Litestar(plugins=[self._host(broker, h)])
        async with self.test_broker_cm(broker), AsyncTestClient(app) as client:
            resp = await client.get("/asyncapi")
            assert resp.status_code == 200
            assert "text/html" in resp.headers["content-type"]

    @pytest.mark.asyncio()
    async def test_asyncapi_json_endpoint_returns_schema(self) -> None:
        broker = self.broker_class(self.broker_url)

        @subscriber("queue-2")
        async def h(payload: dict) -> None: ...

        app = Litestar(plugins=[self._host(broker, h)])
        async with self.test_broker_cm(broker), AsyncTestClient(app) as client:
            resp = await client.get("/asyncapi.json")
            assert resp.status_code == 200
            data = json.loads(resp.content)
            assert "asyncapi" in data
