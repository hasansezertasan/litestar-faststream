import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar import Controller, Litestar
from litestar.exceptions import ImproperlyConfiguredException

from litestar_faststream import FastStreamConfig, FastStreamPlugin
from litestar_faststream.decorators import subscriber
from litestar_faststream.plugin import BrokerConfig


def _fake_broker(name: str = "rabbit") -> MagicMock:
    broker = MagicMock(name=f"broker-{name}")
    broker.connect = AsyncMock()
    broker.start = AsyncMock()
    broker.stop = AsyncMock()
    broker.close = AsyncMock()
    broker._subscribers = []
    broker.__class__.__name__ = "RabbitBroker"

    sub_calls: list = []

    def subscriber_call(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        def deco(fn) -> None:
            sub_calls.append((args, kwargs, fn))
            return fn

        return deco

    broker.subscriber = MagicMock(side_effect=subscriber_call)
    broker.subscriber_calls = sub_calls
    broker.publisher = MagicMock(side_effect=lambda *a, **k: lambda fn: fn)
    broker._faststream_litestar_schema_override = MagicMock(
        title="t",
        description="d",
        version="v",
        to_jsonable=dict,
        to_json=lambda: "{}",
        to_yaml=lambda: "",
    )
    return broker


def _host(*brokers: BrokerConfig) -> FastStreamPlugin:
    return FastStreamPlugin(FastStreamConfig(brokers=list(brokers)))


def test_default_name_from_broker_class_lower() -> None:
    plugin = BrokerConfig(broker=_fake_broker())
    assert plugin.name == "rabbit"


def test_explicit_name_overrides() -> None:
    plugin = BrokerConfig(broker=_fake_broker(), name="primary")
    assert plugin.name == "primary"


def test_unknown_plugin_filter_warns(caplog) -> None:
    @subscriber("q", plugin="bogus")
    async def handler(payload: dict) -> None: ...

    plugin = BrokerConfig(broker=_fake_broker(), handlers=[handler])
    with caplog.at_level("WARNING"):
        Litestar(plugins=[_host(plugin)])
    assert any("bogus" in rec.message for rec in caplog.records)


def test_unknown_plugin_filter_strict_raises() -> None:
    @subscriber("q", plugin="bogus")
    async def handler(payload: dict) -> None: ...

    plugin = BrokerConfig(
        broker=_fake_broker(),
        handlers=[handler],
        strict=True,
    )
    with pytest.raises(ImproperlyConfiguredException, match="bogus"):
        Litestar(plugins=[_host(plugin)])


def test_strict_on_any_broker_raises_for_whole_app() -> None:
    @subscriber("q", plugin="bogus")
    async def handler(payload: dict) -> None: ...

    lax = BrokerConfig(
        broker=_fake_broker("a"),
        name="a",
        handlers=[handler],
    )
    strict = BrokerConfig(
        broker=_fake_broker("b"),
        name="b",
        strict=True,
    )
    with pytest.raises(ImproperlyConfiguredException, match="bogus"):
        Litestar(plugins=[_host(lax, strict)])


def test_strict_does_not_raise_when_all_markers_known() -> None:
    @subscriber("q", plugin="rabbit")
    async def handler(payload: dict) -> None: ...

    plugin = BrokerConfig(
        broker=_fake_broker(),
        handlers=[handler],
        strict=True,
    )
    Litestar(plugins=[_host(plugin)])  # must not raise


def test_known_plugin_filter_does_not_warn(caplog) -> None:
    @subscriber("q", plugin="rabbit")
    async def handler(payload: dict) -> None: ...

    plugin = BrokerConfig(broker=_fake_broker(), handlers=[handler])
    with caplog.at_level("WARNING"):
        Litestar(plugins=[_host(plugin)])
    assert not any("broker name" in rec.message for rec in caplog.records)


def test_duplicate_broker_name_fails_fast() -> None:
    p1 = BrokerConfig(broker=_fake_broker(), name="dup")
    p2 = BrokerConfig(broker=_fake_broker(), name="dup")
    with pytest.raises(ImproperlyConfiguredException, match="Duplicate broker name"):
        FastStreamPlugin(FastStreamConfig(brokers=[p1, p2]))


def test_subscriber_marker_registered_with_broker() -> None:
    @subscriber("queue-x")
    async def handler(payload: dict) -> None: ...

    broker = _fake_broker()
    plugin = BrokerConfig(broker=broker, handlers=[handler])
    Litestar(plugins=[_host(plugin)])
    assert broker.subscriber.called
    args = broker.subscriber.call_args[0]
    assert args == ("queue-x",)


def test_publish_only_propagates_to_composer() -> None:
    plugin = BrokerConfig(broker=_fake_broker(), publish_only=True)
    assert plugin.publish_only is True
    assert plugin._composer.publish_only is True


def test_publish_only_default_is_false() -> None:
    plugin = BrokerConfig(broker=_fake_broker())
    assert plugin.publish_only is False
    assert plugin._composer.publish_only is False


def test_publish_only_warns_when_subscribers_present(caplog) -> None:
    @subscriber("queue-x")
    async def handler(payload: dict) -> None: ...

    plugin = BrokerConfig(
        broker=_fake_broker(),
        handlers=[handler],
        publish_only=True,
    )
    with caplog.at_level("WARNING"):
        Litestar(plugins=[_host(plugin)])
    assert any(
        "publish_only=True" in rec.message and "subscriber" in rec.message
        for rec in caplog.records
    )


def test_publish_only_no_warning_without_subscribers(caplog) -> None:
    plugin = BrokerConfig(
        broker=_fake_broker(),
        publish_only=True,
    )
    with caplog.at_level("WARNING"):
        Litestar(plugins=[_host(plugin)])
    assert not any("publish_only=True" in rec.message for rec in caplog.records)


def test_after_startup_hook_registers() -> None:
    plugin = BrokerConfig(broker=_fake_broker())

    @plugin.after_startup
    async def fn(app) -> None: ...

    assert fn in plugin._composer._after_startup


def test_on_app_init_records_registered_subscribers() -> None:
    @subscriber("queue-x")
    async def handler(payload: dict) -> None: ...

    broker = _fake_broker()
    plugin = BrokerConfig(broker=broker, handlers=[handler])
    Litestar(plugins=[_host(plugin)])

    assert any(
        qual.endswith("handler") and args == ("queue-x",)
        for qual, args, _kw in plugin._registered_subscribers
    )


def test_already_bound_uses_identity_not_qualname() -> None:
    @subscriber("queue-x")
    async def handler(payload: dict) -> None: ...

    broker = _fake_broker()

    # Simulate Tier-1 prior registration: a different function with the same
    # qualname as `handler` but a different identity.
    def _other() -> None: ...

    _other.__qualname__ = handler.__qualname__

    class _Sub:
        fn = staticmethod(_other)

    broker._subscribers = [_Sub()]

    plugin = BrokerConfig(broker=broker, handlers=[handler])
    Litestar(plugins=[_host(plugin)])

    # Identity-based dedup: qualname clash does NOT suppress registration.
    broker.subscriber.assert_called_with("queue-x")


def test_stream_only_controller_instantiated_with_owner_none() -> None:
    """A Controller carrying only ``@subscriber`` methods (no HTTP routes) is
    not reachable through ``app.routes``. The plugin falls back to
    ``controller_cls(owner=None)`` to obtain a single instance for binding.
    Regression coverage for that previously-untested path.
    """
    init_calls: list[object] = []

    class StreamOnly(Controller):
        path = "/unused"

        def __init__(self, owner: object = None) -> None:
            super().__init__(owner=owner)
            init_calls.append(owner)
            self.seen: list[dict] = []

        @subscriber("queue-stream-only")
        async def consume(self, payload: dict) -> None:
            self.seen.append(payload)

    broker = _fake_broker()
    plugin = BrokerConfig(broker=broker, handlers=[StreamOnly])
    host = _host(plugin)
    app = Litestar(plugins=[host])

    # Pre-startup hook normally runs during the lifespan; drive it directly
    # so this remains a unit test (no real broker, no asyncio main loop).
    asyncio.run(plugin._bind_and_register_controllers(app))

    # The Controller was instantiated with ``owner=None`` exactly once and
    # its decorated method was registered against the broker.
    assert init_calls == [None]
    broker.subscriber.assert_called_with("queue-stream-only")


def test_already_bound_scans_all_candidates_when_first_lacks_hint() -> None:
    """Regression: ``_already_bound`` must not bail after one hintless candidate.

    Prior behavior: the first ``_subscribers`` entry that pointed at the
    target function but had no ``_extra_args`` / ``args`` / extractable
    ``queue`` returned ``False`` immediately, so subsequent entries — the
    ones that actually carry the matching queue hint — were never checked.
    """

    @subscriber("queue-x")
    async def handler(payload: dict) -> None: ...

    broker = _fake_broker()

    class _Hintless:
        # Points at the target ``fn`` but exposes no queue hint at all.
        fn = staticmethod(handler)
        _extra_args = None
        args = None
        queue = None

    class _Matching:
        fn = staticmethod(handler)
        _extra_args = ("queue-x",)

    broker._subscribers = [_Hintless(), _Matching()]

    plugin = BrokerConfig(broker=broker, handlers=[handler])
    Litestar(plugins=[_host(plugin)])

    # Loop must reach the matching candidate; no re-registration happens.
    broker.subscriber.assert_not_called()
