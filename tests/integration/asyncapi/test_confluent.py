"""AsyncAPI schema generation tests for Confluent Kafka + Litestar."""

import pytest
from faststream.confluent import KafkaBroker, TestKafkaBroker

from tests.integration.asyncapi.test_base import LitestarAsyncAPITestcase


@pytest.mark.confluent()
class TestConfluentAsyncAPI(LitestarAsyncAPITestcase):
    broker_class = KafkaBroker
    test_broker_cm = TestKafkaBroker
    broker_url = "localhost:9092"
