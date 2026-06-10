"""Tests for the host-plugin variant: same flow as multiple_plugins/."""

import pytest
from faststream.kafka import TestKafkaBroker
from faststream.redis import TestRedisBroker
from litestar.testing import AsyncTestClient

from .app import OrdersController, app, kafka_broker, redis_broker


@pytest.mark.asyncio()
async def test_create_order_bridges_kafka_to_redis() -> None:
    async with (
        TestKafkaBroker(kafka_broker),
        TestRedisBroker(redis_broker),
        AsyncTestClient(app) as client,
    ):
        resp = await client.post("/orders", json={"user_id": 7, "item": "tea"})
        assert resp.status_code == 201
        OrdersController.fanout_notification.mock.assert_called_once()
