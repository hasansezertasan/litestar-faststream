import pytest
from litestar import Controller, get, post
from litestar.exceptions import ImproperlyConfiguredException

from litestar_faststream.decorators import publisher, subscriber
from litestar_faststream.discovery import collect


def test_discovers_module_level_subscriber() -> None:
    @subscriber("q")
    async def fn(payload: dict) -> None: ...

    result = collect(route_handlers=[], extra_handlers=[fn])
    assert len(result.subscribers) == 1
    assert result.subscribers[0][0] is fn
    assert result.subscribers[0][1] == [(("q",), {})]


def test_discovers_subscriber_on_controller_method() -> None:
    class C(Controller):
        path = "/"

        @get("/")
        async def http(self) -> dict:
            return {}

        @subscriber("ctl-queue")
        async def stream(self, payload: dict) -> None: ...

    result = collect(route_handlers=[C], extra_handlers=[])
    # Controller markers are deferred to lifespan binding (so the broker can
    # invoke them as bound methods sharing Litestar's Controller instance).
    # ``collect`` itself emits no entry in ``result.subscribers`` for them.
    assert result.subscribers == []
    assert len(result.controller_subscribers) == 1
    controller_cls, fn, specs = result.controller_subscribers[0]
    assert controller_cls is C
    assert fn is C.__dict__["stream"]
    assert specs == [(("ctl-queue",), {})]


def test_dedup_via_seen_set() -> None:
    @subscriber("q")
    async def fn(payload: dict) -> None: ...

    # Reachable via both route_handlers (callable fallback) and extra_handlers;
    # dedup via seen-set should only register once. Note: Litestar's Router
    # rejects bare functions, so we exercise the dedup path through the
    # callable-fallback branch of _walk_handler instead.
    result = collect(route_handlers=[fn], extra_handlers=[fn])
    assert len(result.subscribers) == 1


def test_publisher_response_mode() -> None:
    @publisher("out")
    async def out_fn(payload: dict) -> dict:
        return {}

    result = collect(route_handlers=[], extra_handlers=[out_fn])
    assert len(result.response_publishers) == 1
    fn, args, kwargs = result.response_publishers[0]
    assert fn is out_fn
    assert args == ("out",)
    assert kwargs == {}


def test_handlers_without_marker_raise() -> None:
    async def fn() -> None: ...

    with pytest.raises(
        ImproperlyConfiguredException,
        match="no @subscriber/@publisher",
    ):
        collect(route_handlers=[], extra_handlers=[fn])


def test_publisher_on_http_handler_rejected() -> None:
    """An HTTP route handler cannot also carry @publisher; raise at discovery."""

    @post("/in")
    @publisher("in-queue")
    async def handler(data: dict) -> dict:
        return {}

    with pytest.raises(
        ImproperlyConfiguredException,
        match="cannot decorate an HTTP route handler",
    ):
        collect(route_handlers=[handler], extra_handlers=[])


def test_subscriber_on_http_handler_rejected() -> None:
    @post("/in")
    @subscriber("in-queue")
    async def handler(data: dict) -> dict:
        return {}

    with pytest.raises(
        ImproperlyConfiguredException,
        match="cannot decorate an HTTP route handler",
    ):
        collect(route_handlers=[handler], extra_handlers=[])


def test_publisher_on_controller_http_method_rejected() -> None:
    class C(Controller):
        path = "/"

        @post("/in")
        @publisher("in-queue")
        async def http_with_pub(self, data: dict) -> dict:
            return {}

    with pytest.raises(
        ImproperlyConfiguredException,
        match="cannot decorate an HTTP route handler",
    ):
        collect(route_handlers=[C], extra_handlers=[])


def test_plugin_filter_claims_matching_subscriber() -> None:
    @subscriber("q", plugin="kafka")
    async def fn(payload: dict) -> None: ...

    result = collect(route_handlers=[], extra_handlers=[fn], plugin_name="kafka")
    assert len(result.subscribers) == 1
    # ``plugin`` key is stripped before forwarding to broker.subscriber(**kwargs)
    assert result.subscribers[0][1] == [(("q",), {})]


def test_plugin_filter_skips_mismatched_subscriber() -> None:
    @subscriber("q", plugin="kafka")
    async def fn(payload: dict) -> None: ...

    result = collect(route_handlers=[], extra_handlers=[fn], plugin_name="redis")
    assert result.subscribers == []


def test_unrouted_subscriber_claimed_by_any_plugin() -> None:
    @subscriber("q")
    async def fn(payload: dict) -> None: ...

    for name in ("kafka", "redis", None):
        result = collect(route_handlers=[], extra_handlers=[fn], plugin_name=name)
        assert len(result.subscribers) == 1


def test_plugin_filter_publisher_response() -> None:
    @publisher("q", plugin="redis")
    async def fn() -> dict:
        return {}

    kept = collect(route_handlers=[], extra_handlers=[fn], plugin_name="redis")
    assert len(kept.response_publishers) == 1
    assert kept.response_publishers[0][2] == {}

    skipped = collect(route_handlers=[], extra_handlers=[fn], plugin_name="kafka")
    assert skipped.response_publishers == []
