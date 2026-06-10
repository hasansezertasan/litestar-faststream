from litestar_faststream.decorators import publisher, subscriber


def test_subscriber_stores_marker_list() -> None:
    @subscriber("queue-a")
    @subscriber("queue-b")
    async def handler(payload: dict) -> None: ...

    assert handler.__faststream_subscribers__ == [
        (("queue-b",), {}),
        (("queue-a",), {}),
    ]


def test_publisher_stores_marker_list() -> None:
    @publisher("out-queue")
    async def handler() -> dict:
        return {}

    assert handler.__faststream_publishers__ == [
        (("out-queue",), {}),
    ]


def test_publisher_extra_kwargs_passthrough() -> None:
    @publisher("q", routing_key="rk", priority=5)
    async def handler() -> None: ...

    assert handler.__faststream_publishers__ == [
        (("q",), {"routing_key": "rk", "priority": 5}),
    ]


def test_subscriber_plugin_filter_stored() -> None:
    @subscriber("q", plugin="kafka")
    async def handler(payload: dict) -> None: ...

    assert handler.__faststream_subscribers__ == [
        (("q",), {"plugin": "kafka"}),
    ]


def test_subscriber_without_plugin_omits_key() -> None:
    @subscriber("q")
    async def handler(payload: dict) -> None: ...

    assert handler.__faststream_subscribers__ == [(("q",), {})]


def test_publisher_plugin_filter_stored() -> None:
    @publisher("q", plugin="redis")
    async def handler() -> dict:
        return {}

    assert handler.__faststream_publishers__ == [
        (("q",), {"plugin": "redis"}),
    ]
