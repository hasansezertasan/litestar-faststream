"""AsyncAPI schema generation tests for Redis + Litestar."""

import pytest
from faststream.redis import RedisBroker, TestRedisBroker

from tests.integration.asyncapi.test_base import LitestarAsyncAPITestcase


@pytest.mark.redis()
class TestRedisAsyncAPI(LitestarAsyncAPITestcase):
    broker_class = RedisBroker
    test_broker_cm = TestRedisBroker
    broker_url = "redis://localhost:6379"
