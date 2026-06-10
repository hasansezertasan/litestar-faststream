"""Integration tests for Rabbit + Litestar."""

from typing import ClassVar

import pytest
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from litestar import Controller, Litestar, get, post
from litestar.testing import AsyncTestClient

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    subscriber,
)
from tests.integration.test_base import LitestarTestcase


def _host(*cfgs: BrokerConfig) -> FastStreamPlugin:
    return FastStreamPlugin(FastStreamConfig(brokers=list(cfgs)))


@pytest.mark.rabbit()
class TestRabbitLitestar(LitestarTestcase):
    broker_class = RabbitBroker
    plugin_class = BrokerConfig
    test_broker_cm = TestRabbitBroker


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_controller_subscriber_runs() -> None:
    broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

    class Ctl(Controller):
        @get("/")
        async def http(self) -> dict:
            return {}

        @staticmethod
        @subscriber("ctl-q")
        async def on_msg(payload: dict) -> None: ...

    app = Litestar(
        plugins=[_host(BrokerConfig(broker=broker))],
        route_handlers=[Ctl],
    )
    async with TestRabbitBroker(broker), AsyncTestClient(app):
        await broker.publish({"x": 1}, queue="ctl-q")
        Ctl.on_msg.mock.assert_called_once_with({"x": 1})


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_controller_subscriber_self_shares_state_with_http() -> None:
    """``@subscriber`` on a Controller method sees the same ``self`` as HTTP.

    Verifies the bind-controller-methods pre-startup hook: the broker invokes
    the stream subscriber as a bound method whose ``__self__`` is the same
    Controller singleton Litestar uses to dispatch HTTP. State mutated by an
    HTTP request is therefore observable to subsequent broker messages on the
    same instance, and vice versa.
    """
    broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

    class Counter(Controller):
        path = "/"
        events: ClassVar[list[tuple[str, int]]] = []

        @get("/bump")
        async def http_bump(self) -> dict:
            self.events.append(("http", id(self)))
            return {}

        @subscriber("counter-q")
        async def on_msg(self, payload: dict) -> None:
            self.events.append(("stream", id(self)))

    app = Litestar(
        plugins=[_host(BrokerConfig(broker=broker))],
        route_handlers=[Counter],
    )

    async with TestRabbitBroker(broker), AsyncTestClient(app) as client:
        await client.get("/bump")
        await broker.publish({"by": 5}, queue="counter-q")
        await client.get("/bump")

    assert [path for path, _ in Counter.events] == ["http", "stream", "http"]
    ids = {sid for _, sid in Counter.events}
    assert len(ids) == 1, (
        "HTTP and stream must share Litestar's Controller singleton; "
        f"got distinct self ids: {Counter.events}"
    )


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_stream_only_controller_uses_plugin_instance() -> None:
    """Controller with only stream methods (no HTTP routes) still gets ``self``.

    Litestar instantiates such Controllers but discards the instance (no
    bound HTTP method retains a reference). The plugin's pre-startup hook
    falls back to creating its own instance so stream handlers can use
    ``self`` without a TypeError.
    """
    broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

    class StreamOnly(Controller):
        path = "/"
        seen: ClassVar[list[dict]] = []

        @subscriber("stream-only-q")
        async def on_msg(self, payload: dict) -> None:
            self.seen.append(payload)

    app = Litestar(
        plugins=[_host(BrokerConfig(broker=broker))],
        route_handlers=[StreamOnly],
    )

    async with TestRabbitBroker(broker), AsyncTestClient(app):
        await broker.publish({"x": 1}, queue="stream-only-q")
        await broker.publish({"x": 2}, queue="stream-only-q")

    assert StreamOnly.seen == [{"x": 1}, {"x": 2}]


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_http_handler_publishes_via_di() -> None:
    """HTTP handler injects the broker and calls ``publish`` directly.

    This replaces the old ``@publisher(source="request")`` shortcut. The
    handler stays in control of ordering and error handling; the receiving
    subscriber is just a normal ``@subscriber``.
    """
    broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

    @subscriber("send-email")
    async def process(data: dict) -> None: ...

    @post("/send-email")
    async def endpoint(data: dict, rabbit: RabbitBroker) -> dict:
        await rabbit.publish(data, queue="send-email")
        return {"queued": True}

    app = Litestar(
        plugins=[_host(BrokerConfig(broker=broker, handlers=[process]))],
        route_handlers=[endpoint],
    )

    async with TestRabbitBroker(broker), AsyncTestClient(app) as client:
        resp = await client.post("/send-email", json={"subject": "hi"})
        assert resp.status_code == 201
        process.mock.assert_called_once_with({"subject": "hi"})


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_http_handler_receives_broker_via_di() -> None:
    """Handler injects broker via Litestar DI under the broker config's name.

    ``BrokerConfig`` registers the broker under ``self.name`` (default: broker
    class name lower-cased — ``rabbit``). Handlers opt-in by declaring a
    parameter of that name. The literal key ``"broker"`` is intentionally
    NOT registered to avoid collisions across multiple brokers.
    """
    broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

    @subscriber("di-target")
    async def consumer(payload: dict) -> None: ...

    @get("/trigger")
    async def trigger(rabbit: RabbitBroker) -> dict:
        await rabbit.publish({"hello": "world"}, queue="di-target")
        return {"published": True}

    app = Litestar(
        plugins=[_host(BrokerConfig(broker=broker, handlers=[consumer]))],
        route_handlers=[trigger],
    )
    async with TestRabbitBroker(broker), AsyncTestClient(app) as client:
        resp = await client.get("/trigger")
        assert resp.status_code == 200
        assert resp.json() == {"published": True}
        consumer.mock.assert_called_once_with({"hello": "world"})


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_after_startup_hook_runs_after_broker_start() -> None:
    broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
    cfg = BrokerConfig(broker=broker)
    order: list[str] = []

    @cfg.after_startup
    async def hook(app: object) -> None:
        order.append("after")

    app = Litestar(plugins=[_host(cfg)])
    async with TestRabbitBroker(broker), AsyncTestClient(app):
        pass
    assert order == ["after"]
