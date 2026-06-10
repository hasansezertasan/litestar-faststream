"""AsyncAPI schema generation tests for Rabbit + Litestar."""

import pytest
from faststream.rabbit import RabbitBroker, TestRabbitBroker

from tests.integration.asyncapi.test_base import LitestarAsyncAPITestcase


@pytest.mark.rabbit()
class TestRabbitAsyncAPI(LitestarAsyncAPITestcase):
    broker_class = RabbitBroker
    test_broker_cm = TestRabbitBroker
    broker_url = "amqp://guest:guest@localhost:5672/"
