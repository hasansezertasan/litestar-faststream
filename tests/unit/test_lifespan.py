from unittest.mock import AsyncMock

import pytest

from litestar_faststream.lifespan import LifespanComposer


class _FakeBroker:
    def __init__(self) -> None:
        self.connect = AsyncMock()
        self.start = AsyncMock()
        self.stop = AsyncMock()
        self.close = AsyncMock()


@pytest.mark.asyncio()
async def test_compose_starts_and_stops_broker() -> None:
    broker = _FakeBroker()
    after = AsyncMock()
    on_shutdown = AsyncMock()

    composer = LifespanComposer(broker)
    composer.add_after_startup(after)
    composer.add_on_broker_shutdown(on_shutdown)

    cm = composer.build()
    async with cm("app") as extras:
        assert extras == {"broker": broker}
        broker.start.assert_awaited_once()
        after.assert_awaited_once_with("app")

    on_shutdown.assert_awaited_once_with("app")
    broker.stop.assert_awaited_once()


@pytest.mark.asyncio()
async def test_publish_only_skips_broker_start() -> None:
    broker = _FakeBroker()
    composer = LifespanComposer(broker, publish_only=True)

    async with composer.build()("app") as extras:
        assert extras == {"broker": broker}
        broker.connect.assert_awaited_once()
        broker.start.assert_not_awaited()

    broker.stop.assert_awaited_once()


@pytest.mark.asyncio()
async def test_hook_failure_does_not_block_broker_stop() -> None:
    broker = _FakeBroker()
    bad = AsyncMock(side_effect=RuntimeError("boom"))

    composer = LifespanComposer(broker)
    composer.add_on_broker_shutdown(bad)

    async with composer.build()("app"):
        pass

    bad.assert_awaited()
    broker.stop.assert_awaited_once()
