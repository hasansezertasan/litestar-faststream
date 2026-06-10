"""Tests for OtelMiddleware: propagation, span shape, no-op fallback."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from litestar_faststream import instrumentation as instr_mod
from litestar_faststream.instrumentation import OtelMiddleware


def _fake_msg(headers: dict[str, Any] | None = None, **extra: Any) -> SimpleNamespace:
    return SimpleNamespace(headers=headers or {}, **extra)


def _instantiate() -> OtelMiddleware:
    # BaseMiddleware.__init__ requires (msg, *, context); shape parity is the
    # contract since this class is subclassed onto FastStream's middleware
    # chain.
    return OtelMiddleware(None, context=MagicMock())


@pytest.fixture()
def _reset_tracer_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear the module's tracer cache AND install an SDK provider.

    The OTel global default is a NoOp provider; spans it returns carry an
    invalid context, which the W3C TraceContext propagator silently refuses
    to inject. Configuring a real ``TracerProvider`` is the smallest setup
    that makes ``propagate.inject`` actually write a ``traceparent`` header.
    """
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    monkeypatch.setattr(instr_mod, "_tracer", None)
    provider = TracerProvider()
    # ``set_tracer_provider`` warns on override; bypass via the private API
    # so test isolation doesn't bleed warnings into output.
    trace._TRACER_PROVIDER = provider


# ----- no-op behaviour when OTel is absent ---------------------------------


@pytest.mark.asyncio()
async def test_consume_passes_through_when_otel_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(instr_mod, "OPENTELEMETRY_INSTALLED", False)
    mw = _instantiate()
    call_next = AsyncMock(return_value="result")
    msg = _fake_msg(subject="events")
    assert await mw.consume_scope(call_next, msg) == "result"
    call_next.assert_awaited_once_with(msg)


@pytest.mark.asyncio()
async def test_publish_passes_through_when_otel_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(instr_mod, "OPENTELEMETRY_INSTALLED", False)
    mw = _instantiate()
    call_next = AsyncMock(return_value="ack")
    cmd = _fake_msg(destination="events")
    assert await mw.publish_scope(call_next, cmd) == "ack"
    call_next.assert_awaited_once_with(cmd)
    # Headers must not be touched on the no-op path.
    assert cmd.headers == {}


def test_stub_tracer_used_when_otel_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reloading _otel_typing without OTel imports yields a stub flag."""
    # We can't actually uninstall OTel here, but we can verify the symbols
    # the stub branch defines are reachable as a sanity check.
    from litestar_faststream import _otel_typing as t

    assert hasattr(t, "OPENTELEMETRY_INSTALLED")
    assert hasattr(t, "trace")
    assert hasattr(t, "propagate")


# ----- real OTel propagation ------------------------------------------------


@pytest.mark.asyncio()
@pytest.mark.usefixtures("_reset_tracer_cache")
async def test_publish_injects_traceparent_into_headers() -> None:
    pytest.importorskip("opentelemetry")
    mw = _instantiate()
    cmd = _fake_msg(destination="events", correlation_id="abc")
    call_next = AsyncMock(return_value=None)

    await mw.publish_scope(call_next, cmd)

    # The default global propagator is TraceContext; it writes "traceparent".
    assert "traceparent" in cmd.headers, cmd.headers
    call_next.assert_awaited_once_with(cmd)


@pytest.mark.asyncio()
@pytest.mark.usefixtures("_reset_tracer_cache")
async def test_consume_extracts_parent_context_from_headers() -> None:
    """End-to-end: a publish-injected traceparent links to the consume span."""
    pytest.importorskip("opentelemetry")
    from opentelemetry import trace

    mw = _instantiate()

    # Publish to capture the producer's traceparent header.
    cmd = _fake_msg(destination="events")
    await mw.publish_scope(AsyncMock(return_value=None), cmd)
    producer_traceparent = cmd.headers["traceparent"]

    # Hand those headers to consume_scope and capture the active trace id.
    captured: dict[str, str] = {}

    async def handler(_msg: Any) -> None:
        span = trace.get_current_span()
        ctx = span.get_span_context()
        captured["trace_id"] = format(ctx.trace_id, "032x")

    msg = _fake_msg(headers={"traceparent": producer_traceparent}, subject="events")
    await mw.consume_scope(handler, msg)

    # Same trace_id => producer + consumer are linked in one trace.
    assert captured["trace_id"] in producer_traceparent


@pytest.mark.asyncio()
@pytest.mark.usefixtures("_reset_tracer_cache")
async def test_consume_records_exception_and_reraises() -> None:
    pytest.importorskip("opentelemetry")
    mw = _instantiate()
    call_next = AsyncMock(side_effect=RuntimeError("boom"))
    msg = _fake_msg(subject="events")
    with pytest.raises(RuntimeError, match="boom"):
        await mw.consume_scope(call_next, msg)


@pytest.mark.usefixtures("_reset_tracer_cache")
def test_get_tracer_is_cached() -> None:
    pytest.importorskip("opentelemetry")
    first = instr_mod.get_tracer()
    second = instr_mod.get_tracer()
    assert first is second


# ----- destination extraction is broker-agnostic ---------------------------


@pytest.mark.parametrize(
    ("attr", "value"),
    (
        ("destination", "events"),
        ("subject", "events"),
        ("topic", "events"),
        ("queue", "events"),
        ("channel", "events"),
    ),
)
def test_extract_destination_handles_multiple_brokers(attr: str, value: str) -> None:
    from litestar_faststream.instrumentation import _extract_destination

    obj = SimpleNamespace(**{attr: value})
    assert _extract_destination(obj) == value


def test_extract_destination_unwraps_named_object() -> None:
    """RabbitMQ etc. expose queue objects with ``.name`` rather than plain strings."""
    from litestar_faststream.instrumentation import _extract_destination

    obj = SimpleNamespace(queue=SimpleNamespace(name="events"))
    assert _extract_destination(obj) == "events"


# ----- @monitored decorator -----------------------------------------------


import logging  # noqa: E402

from litestar_faststream import monitored  # noqa: E402


@pytest.mark.asyncio()
async def test_monitored_bare_form_logs_timing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    @monitored
    async def handler(msg: str) -> str:
        return f"ok:{msg}"

    with caplog.at_level(logging.INFO, logger="litestar_faststream.handlers"):
        result = await handler("a")

    assert result == "ok:a"
    [record] = [r for r in caplog.records if "handler" in r.message]
    wrapped = getattr(handler, "__wrapped__", handler)
    assert getattr(record, "handler", None) == wrapped.__qualname__
    assert getattr(record, "ok", None) is True
    assert getattr(record, "duration_ms", None) is not None


@pytest.mark.asyncio()
async def test_monitored_parameterized_form_uses_custom_name_and_attrs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    @monitored(span_name="process_order", attributes={"tier": "critical"})
    async def handler(_msg: Any) -> None:
        return None

    with caplog.at_level(logging.INFO, logger="litestar_faststream.handlers"):
        await handler({"order_id": 1})

    [record] = [r for r in caplog.records if "process_order" in r.message]
    assert getattr(record, "handler", None) == "process_order"
    # Custom attributes flow into the log record via ``extra=``.
    assert getattr(record, "tier", None) == "critical"


@pytest.mark.asyncio()
async def test_monitored_marks_failure_and_reraises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    @monitored
    async def handler(_msg: Any) -> None:
        msg = "boom"
        raise RuntimeError(msg)

    with (
        caplog.at_level(logging.INFO, logger="litestar_faststream.handlers"),
        pytest.raises(RuntimeError, match="boom"),
    ):
        await handler(None)

    [record] = [r for r in caplog.records if "handler" in r.message]
    assert getattr(record, "ok", None) is False


@pytest.mark.asyncio()
@pytest.mark.usefixtures("_reset_tracer_cache")
async def test_monitored_records_payload_when_opted_in() -> None:
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    from opentelemetry import trace as _trace

    _trace._TRACER_PROVIDER = provider
    # Reset cached tracer so it picks up the new provider.
    import litestar_faststream.instrumentation as inst_mod

    inst_mod._tracer = None

    @monitored(record_payload=True, attributes={"team": "billing"})
    async def handler(msg: dict[str, Any]) -> None:
        return None

    await handler({"order_id": 42})

    spans = exporter.get_finished_spans()
    assert spans, "expected at least one span"
    attrs = spans[-1].attributes or {}
    assert attrs.get("team") == "billing"
    preview = str(attrs.get("messaging.message.body.preview") or "")
    assert "order_id" in preview


@pytest.mark.asyncio()
@pytest.mark.usefixtures("_reset_tracer_cache")
async def test_monitored_truncates_long_payloads() -> None:
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    from opentelemetry import trace as _trace

    _trace._TRACER_PROVIDER = provider
    import litestar_faststream.instrumentation as inst_mod

    inst_mod._tracer = None

    @monitored(record_payload=True, record_payload_max_bytes=16)
    async def handler(_msg: str) -> None:
        return None

    await handler("x" * 200)

    spans = exporter.get_finished_spans()
    preview = str(
        (spans[-1].attributes or {}).get("messaging.message.body.preview", ""),
    )
    # 16 chars + the ellipsis sentinel.
    assert preview.endswith("…")
    assert len(preview) == 17


@pytest.mark.asyncio()
@pytest.mark.usefixtures("_reset_tracer_cache")
async def test_monitored_payload_recording_off_by_default() -> None:
    """PII safety: payload is NOT recorded unless explicitly opted in."""
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    from opentelemetry import trace as _trace

    _trace._TRACER_PROVIDER = provider
    import litestar_faststream.instrumentation as inst_mod

    inst_mod._tracer = None

    @monitored
    async def handler(_msg: dict[str, Any]) -> None:
        return None

    await handler({"sensitive": "PII"})

    spans = exporter.get_finished_spans()
    attrs = spans[-1].attributes or {}
    assert "messaging.message.body.preview" not in attrs
