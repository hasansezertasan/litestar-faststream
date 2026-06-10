"""AsyncAPI schema generation tests for MQTT + Litestar."""

import pytest

# zmqtt (third-party transport required by faststream.mqtt) is only
# available on Python >= 3.11; skip the entire module elsewhere so test
# collection doesn't fail on older interpreters.
pytest.importorskip("zmqtt")

from faststream.mqtt import MQTTBroker, TestMQTTBroker

from tests.integration.asyncapi.test_base import LitestarAsyncAPITestcase


@pytest.mark.mqtt()
class TestMQTTAsyncAPI(LitestarAsyncAPITestcase):
    broker_class = MQTTBroker
    test_broker_cm = TestMQTTBroker
    broker_url = "localhost"
