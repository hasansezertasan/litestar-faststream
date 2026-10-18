from unittest.mock import AsyncMock

import pytest

from litestar_faststream.lifespan import (
    LifespanComposer,
    run_broker_lifecycle_shutdown,
    run_broker_lifecycle_startup,
)


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


@pytest.mark.asyncio()
async def test_failed_startup_stops_broker_and_reraises() -> None:
    broker = _FakeBroker()
    broker.start.side_effect = RuntimeError("start failed")

    with pytest.raises(RuntimeError, match="start failed"):
        async with LifespanComposer(broker).build()("app"):
            pass  # pragma: no cover

    broker.stop.assert_awaited_once()


@pytest.mark.asyncio()
async def test_failed_startup_survives_stop_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = _FakeBroker()
    broker.connect.side_effect = RuntimeError("connect failed")
    broker.stop.side_effect = RuntimeError("stop failed")

    with pytest.raises(RuntimeError, match="connect failed"):
        await run_broker_lifecycle_startup(broker)

    assert any("after failed startup" in r.message for r in caplog.records)


@pytest.mark.asyncio()
async def test_shutdown_logs_and_swallows_stop_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = _FakeBroker()
    broker.stop.side_effect = RuntimeError("stop failed")

    await run_broker_lifecycle_shutdown(broker)

    assert any("broker.stop raised" in r.message for r in caplog.records)


@pytest.mark.asyncio()
async def test_after_startup_failure_tears_down_broker() -> None:
    broker = _FakeBroker()
    composer = LifespanComposer(broker)
    composer.add_after_startup(AsyncMock(side_effect=RuntimeError("hook failed")))

    with pytest.raises(RuntimeError, match="hook failed"):
        async with composer.build()("app"):
            pass  # pragma: no cover

    broker.stop.assert_awaited_once()


@pytest.mark.asyncio()
async def test_pre_startup_hook_runs_before_connect() -> None:
    broker = _FakeBroker()
    order: list[str] = []
    broker.connect.side_effect = lambda: order.append("connect")

    async def pre(app: object) -> None:
        order.append("pre")

    composer = LifespanComposer(broker)
    composer.add_pre_startup(pre)

    async with composer.build()("app"):
        pass

    assert order == ["pre", "connect"]
