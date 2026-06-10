"""Tests for the example app -- assert publish_to bridge fires."""

import pytest
from faststream.rabbit import TestRabbitBroker
from litestar.testing import AsyncTestClient

from .app import OrdersController, app, broker


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_create_order_publishes_to_queue() -> None:
    async with TestRabbitBroker(broker), AsyncTestClient(app) as client:
        resp = await client.post("/orders", json={"user_id": 7, "item": "tea"})
        assert resp.status_code == 201
        OrdersController.on_order.mock.assert_called_once()
        # Regression: @publisher("orders.processed") on on_order must route
        # the return value into on_processed via the shared HandlerCallWrapper.
        OrdersController.on_processed.mock.assert_called_once()
