"""Lifespan composition for BrokerConfig: wrap user lifespan, run hooks."""

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

logger = logging.getLogger(__name__)

LifespanHook = Callable[[Any], Awaitable[None]]


async def run_broker_lifecycle_startup(
    broker: Any,
    *,
    publish_only: bool = False,
) -> None:
    """Connect (and optionally start); on failure, attempt stop so resources aren't leaked.

    When ``publish_only=True``, only ``broker.connect()`` is called and the
    subscriber consume-loops are not started. ``broker.publish(...)`` still
    works because it requires only an open connection. Subscribers already
    registered on the broker are left in place (so AsyncAPI reflects the full
    surface) but will not consume messages in this process.
    """
    try:
        await broker.connect()
        if not publish_only:
            await broker.start()
    except BaseException:
        try:
            await broker.stop()
        except Exception:
            logger.exception("broker.stop after failed startup raised")
        raise


async def run_broker_lifecycle_shutdown(broker: Any) -> None:
    try:
        await broker.stop()
    except Exception:
        logger.exception("broker.stop raised")


class LifespanComposer:
    def __init__(
        self,
        broker: Any,
        *,
        state_key: str = "broker",
        publish_only: bool = False,
    ) -> None:
        self.broker = broker
        self.state_key = state_key
        self.publish_only = publish_only
        self._pre_startup: list[LifespanHook] = []
        self._after_startup: list[LifespanHook] = []
        self._on_shutdown: list[LifespanHook] = []

    def add_pre_startup(self, fn: LifespanHook) -> None:
        """Register a hook that runs BEFORE ``broker.connect()/start()``.

        Used to bind Controller @subscriber/@publisher methods to Litestar's
        own Controller instances (reachable only after Litestar finishes
        building ``app.routes``) and register them with the broker before
        ``broker.start()`` iterates its subscriber set.
        """
        self._pre_startup.append(fn)

    def add_after_startup(self, fn: LifespanHook) -> None:
        self._after_startup.append(fn)

    def add_on_broker_shutdown(self, fn: LifespanHook) -> None:
        self._on_shutdown.append(fn)

    def build(
        self,
    ) -> Callable[[Any], AbstractAsyncContextManager[Mapping[str, Any]]]:
        broker = self.broker
        state_key = self.state_key
        publish_only = self.publish_only
        pre = self._pre_startup
        after = self._after_startup
        on_shutdown = self._on_shutdown

        @asynccontextmanager
        async def cm(app: Any) -> AsyncIterator[Mapping[str, Any]]:
            for hook in pre:
                await hook(app)
            await run_broker_lifecycle_startup(broker, publish_only=publish_only)
            try:
                for hook in after:
                    await hook(app)
            except BaseException:
                # Startup-hook failure leaves the app half-initialised; tear
                # down the broker before propagating so resources don't leak.
                await run_broker_lifecycle_shutdown(broker)
                raise
            try:
                yield {state_key: broker}
            finally:
                for hook in on_shutdown:
                    try:
                        await hook(app)
                    except Exception:  # noqa: PERF203
                        logger.exception(
                            "on_broker_shutdown hook %s raised",
                            getattr(hook, "__qualname__", repr(hook)),
                        )
                await run_broker_lifecycle_shutdown(broker)

        return cm
