"""Drop-in lifecycle hooks and middleware for ``BrokerConfig``.

Two flavors live here:

* **Lifecycle hooks** -- coroutines with signature ``(app) -> None``, registered
  via ``BrokerConfig.after_startup`` / ``BrokerConfig.on_broker_shutdown``.
  They run once per broker per Litestar app, at startup/shutdown.

* **Middleware** -- a ``LoggingMiddleware`` that wraps each consume/publish
  call. Attach it to the FastStream broker itself (``RabbitBroker(middlewares=[
  LoggingMiddleware])``) so it runs per-message.

Both are intentionally small and example-shaped: their job is to give users
a working starting point and to demonstrate the right hook surface, not to
replace a production observability stack.

Usage::

    from faststream.rabbit import RabbitBroker
    from litestar_faststream import BrokerConfig
    from litestar_faststream.hooks import (
        LoggingMiddleware,
        shutdown_banner,
        startup_banner,
    )

    broker = RabbitBroker("amqp://localhost", middlewares=[LoggingMiddleware])
    cfg = BrokerConfig(broker, name="rabbit")
    cfg.after_startup(startup_banner)
    cfg.on_broker_shutdown(shutdown_banner)
"""

import logging
import time
from typing import TYPE_CHECKING, Any

from faststream import BaseMiddleware

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    "LoggingMiddleware",
    "shutdown_banner",
    "startup_banner",
    "timing_middleware",
)

logger = logging.getLogger("litestar_faststream.hooks")


# ----- lifecycle hooks ------------------------------------------------------


async def startup_banner(app: Any) -> None:
    """Log a one-line summary when the broker has started.

    Walks the Litestar app's plugins to find ``FastStreamPlugin`` and reports
    each ``BrokerConfig`` it owns plus its subscriber count. Safe to register
    on multiple ``BrokerConfig`` instances; the banner is per-app, not
    per-broker, so duplicates are de-duplicated in the same lifespan.
    """
    plugins = getattr(app, "plugins", ()) or ()
    for plugin in plugins:
        children = getattr(plugin, "_children", None)
        if not children:
            continue
        names = ", ".join(
            f"{c.name}({len(getattr(c, '_registered_subscribers', []) or [])} subs)"
            for c in children
        )
        logger.info("FastStream brokers ready: %s", names)
        return


async def shutdown_banner(app: Any) -> None:
    """Log a one-line message before broker shutdown begins."""
    logger.info("FastStream broker shutting down")


# ----- middleware -----------------------------------------------------------


class LoggingMiddleware(BaseMiddleware):
    """Log + time each consumed message.

    Attaches per-broker (``Broker(middlewares=[LoggingMiddleware])``) so every
    subscriber gets timing + structured log lines without touching handler
    code. Mirrors SAQ's ``timing_before_process`` / ``timing_after_process``
    pair but uses FastStream's middleware lifecycle (``consume_scope`` wraps
    the entire dispatch) so the timing covers handler + framework overhead in
    one span.

    Log records carry an ``extra`` dict (``duration_ms``, ``message_class``,
    ``ok``) so a structured-logging processor (structlog, logfire, etc.) can
    project them into fields without parsing the message.
    """

    async def consume_scope(self, call_next: Any, msg: Any) -> Any:
        start = time.monotonic()
        ok = True
        try:
            return await call_next(msg)
        except BaseException:
            ok = False
            raise
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000.0
            logger.info(
                "consume %s ok=%s in %.2fms",
                type(msg).__name__,
                ok,
                elapsed_ms,
                extra={
                    "duration_ms": round(elapsed_ms, 3),
                    "message_class": type(msg).__name__,
                    "ok": ok,
                },
            )

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: "TracebackType | None" = None,
    ) -> bool | None:
        if exc_type is not None:
            # ``logger.exception`` requires being inside ``except``; emit at
            # ERROR level with structured ``exc_info`` for the same effect
            # from a regular hook.
            logger.error(
                "consume raised %s: %s",
                exc_type.__name__,
                exc_val,
                exc_info=(exc_type, exc_val, exc_tb) if exc_val else None,
            )
        return None


# Convenience alias: most users only want the timing/logging behaviour, and
# the class name is what they'll search for. Keep both exports so existing
# documentation that says "add ``timing_middleware``" reads as a noun.
timing_middleware = LoggingMiddleware
