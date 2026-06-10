"""Tests for the example app -- assert publish_to bridge fires."""

import pytest

# zmqtt (third-party transport required by faststream.mqtt) is only
# available on Python >= 3.11; skip the entire module elsewhere so test
# collection doesn't fail on older interpreters.
pytest.importorskip("zmqtt")

from faststream.mqtt import TestMQTTBroker
from litestar.testing import AsyncTestClient

from .app import OrdersController, app, broker


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_create_order_publishes_to_topic() -> None:
    async with TestMQTTBroker(broker), AsyncTestClient(app) as client:
        resp = await client.post("/orders", json={"user_id": 7, "item": "tea"})
        assert resp.status_code == 201
        OrdersController.on_order.mock.assert_called_once()
        # Regression: @publisher("orders/processed") on on_order must route
        # the return value into on_processed via the shared HandlerCallWrapper.
        OrdersController.on_processed.mock.assert_called_once()
