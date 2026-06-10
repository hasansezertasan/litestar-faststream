"""AsyncAPI schema generation tests for NATS + Litestar."""

import pytest
from faststream.nats import NatsBroker, TestNatsBroker

from tests.integration.asyncapi.test_base import LitestarAsyncAPITestcase


@pytest.mark.nats()
class TestNatsAsyncAPI(LitestarAsyncAPITestcase):
    broker_class = NatsBroker
    test_broker_cm = TestNatsBroker
    broker_url = "nats://localhost:4222"
