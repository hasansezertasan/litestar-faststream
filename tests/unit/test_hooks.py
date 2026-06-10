"""Tests for the drop-in hooks/middleware in ``litestar_faststream.hooks``."""

import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from litestar_faststream.hooks import (
    LoggingMiddleware,
    shutdown_banner,
    startup_banner,
    timing_middleware,
)


def _fake_app(plugins: list[Any]) -> SimpleNamespace:
    """A stand-in for ``Litestar``: only ``.plugins`` is read by the hooks."""
    return SimpleNamespace(plugins=plugins)


def _fake_plugin(*children: Any) -> SimpleNamespace:
    return SimpleNamespace(_children=list(children))


def _fake_child(name: str, sub_count: int) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        _registered_subscribers=[("h", (), {})] * sub_count,
    )


@pytest.mark.asyncio()
async def test_startup_banner_logs_broker_summary(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = _fake_app([
        _fake_plugin(_fake_child("rabbit", 3), _fake_child("redis", 1)),
    ])
    with caplog.at_level(logging.INFO, logger="litestar_faststream.hooks"):
        await startup_banner(app)
    assert any("rabbit(3 subs)" in r.message for r in caplog.records)
    assert any("redis(1 subs)" in r.message for r in caplog.records)


@pytest.mark.asyncio()
async def test_startup_banner_no_plugin_is_noop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An app without FastStreamPlugin must not raise -- hooks ship as drop-ins."""
    app = _fake_app([])
    with caplog.at_level(logging.INFO, logger="litestar_faststream.hooks"):
        await startup_banner(app)
    assert caplog.records == []


@pytest.mark.asyncio()
async def test_shutdown_banner_logs(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="litestar_faststream.hooks"):
        await shutdown_banner(_fake_app([]))
    assert any("shutting down" in r.message for r in caplog.records)


@pytest.mark.asyncio()
async def test_logging_middleware_records_duration_and_ok(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # FastStream's BaseMiddleware __init__ takes (msg, *, context); shape
    # parity matters because subclassing the real type is the contract.
    mw = LoggingMiddleware(None, context=MagicMock())
    next_call = AsyncMock(return_value="result")

    with caplog.at_level(logging.INFO, logger="litestar_faststream.hooks"):
        result = await mw.consume_scope(next_call, msg=object())

    assert result == "result"
    next_call.assert_awaited_once()
    [record] = [r for r in caplog.records if "consume" in r.message]
    assert getattr(record, "ok", None) is True
    assert getattr(record, "duration_ms", None) is not None
    assert getattr(record, "message_class", None) == "object"


@pytest.mark.asyncio()
async def test_logging_middleware_marks_failure_and_reraises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    mw = LoggingMiddleware(None, context=MagicMock())
    next_call = AsyncMock(side_effect=RuntimeError("boom"))

    with (
        caplog.at_level(logging.INFO, logger="litestar_faststream.hooks"),
        pytest.raises(RuntimeError, match="boom"),
    ):
        await mw.consume_scope(next_call, msg=object())

    [record] = [r for r in caplog.records if "consume" in r.message]
    assert getattr(record, "ok", None) is False


def test_timing_middleware_alias_points_at_logging_middleware() -> None:
    """The convenience alias must stay bound to the real class."""
    assert timing_middleware is LoggingMiddleware
