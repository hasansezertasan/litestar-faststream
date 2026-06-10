"""AsyncAPI schema generation tests for Kafka (aiokafka) + Litestar."""

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from tests.integration.asyncapi.test_base import LitestarAsyncAPITestcase


@pytest.mark.kafka()
class TestKafkaAsyncAPI(LitestarAsyncAPITestcase):
    broker_class = KafkaBroker
    test_broker_cm = TestKafkaBroker
    broker_url = "localhost:9092"
