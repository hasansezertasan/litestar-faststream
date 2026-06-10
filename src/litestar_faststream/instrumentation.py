"""OpenTelemetry instrumentation for litestar-faststream.

Provides :class:`OtelMiddleware`, a FastStream middleware that:

* On **publish**, starts a PRODUCER span and injects the current W3C trace
  context into the outgoing message headers (``cmd.headers``). The consumer
  (in this process or a downstream service) can then continue the trace.

* On **consume**, extracts the trace context from incoming message headers
  and starts a CONSUMER span as a child of that context. A Litestar HTTP
  request that publishes a message therefore produces a single connected
  trace spanning HTTP -> publish -> consume -> handler.

Attribute names follow the OpenTelemetry semantic conventions for messaging
systems (``messaging.system``, ``messaging.destination.name``,
``messaging.operation``). The middleware is a no-op when ``opentelemetry``
is not installed, so it's safe to register unconditionally.

Usage::

    from faststream.rabbit import RabbitBroker
    from litestar_faststream import BrokerConfig
    from litestar_faststream.instrumentation import OtelMiddleware

    broker = RabbitBroker("amqp://localhost", middlewares=[OtelMiddleware])
    cfg = BrokerConfig(broker, name="rabbit")
"""

import logging
import time
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

from faststream import BaseMiddleware
from typing_extensions import ParamSpec

from ._otel_typing import (
    OPENTELEMETRY_INSTALLED,
    SpanKind,
    Status,
    StatusCode,
    propagate,
    trace,
)

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    "OPENTELEMETRY_INSTALLED",
    "OtelMiddleware",
    "get_tracer",
    "monitored",
)

_log = logging.getLogger("litestar_faststream.handlers")

# Truncate payloads attached to spans / logs so a 4MB Kafka message doesn't
# explode the exporter. 1 KiB is the common OTel guidance for ``messaging``
# attributes; users can raise it via ``record_payload_max_bytes``.
_DEFAULT_PAYLOAD_TRUNCATION = 1024

P = ParamSpec("P")
R = TypeVar("R")

TRACER_NAME = "litestar_faststream"

_tracer: Any = None


def get_tracer() -> Any:
    """Return the package tracer, creating it lazily.

    Returns a no-op stub when OpenTelemetry isn't installed. Callers can
    therefore always ``with tracer.start_as_current_span(...)`` without
    feature-detecting first.
    """
    global _tracer  # noqa: PLW0603 - intentional module-level cache
    if _tracer is None:
        _tracer = trace.get_tracer(TRACER_NAME)
    return _tracer


def _extract_destination(obj: Any) -> str:
    """Best-effort destination name (queue / topic / subject) for span attrs."""
    for attr in ("destination", "subject", "topic", "queue", "channel"):
        val = getattr(obj, attr, None)
        if val:
            return str(getattr(val, "name", val))
    return ""


class OtelMiddleware(BaseMiddleware):
    """Trace consume + publish operations with W3C context propagation.

    Spans created here use OTel messaging semantic conventions::

        messaging.system           = "faststream"
        messaging.destination.name = <queue/topic/subject>
        messaging.operation        = "publish" | "process"
        messaging.message.id       = <correlation_id>  (when available)

    Exceptions raised by handlers / publishers are recorded on the active
    span (``span.record_exception``) and the span's status is set to ERROR
    before the exception propagates.
    """

    async def consume_scope(self, call_next: Any, msg: Any) -> Any:
        if not OPENTELEMETRY_INSTALLED:
            return await call_next(msg)

        # Pull W3C traceparent out of message headers; propagate.extract
        # returns the *current* context unchanged when no carrier keys match,
        # so even untraced upstreams yield a valid (root) span.
        headers: dict[str, Any] = dict(getattr(msg, "headers", None) or {})
        parent_ctx = propagate.extract(carrier=headers)

        tracer = get_tracer()
        destination = _extract_destination(msg)
        span_name = f"{destination} process" if destination else "faststream process"
        with tracer.start_as_current_span(
            span_name,
            kind=SpanKind.CONSUMER,
            context=parent_ctx,
        ) as span:
            self._set_message_attrs(span, msg, operation="process")
            try:
                return await call_next(msg)
            except BaseException as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    async def publish_scope(self, call_next: Any, cmd: Any) -> Any:
        if not OPENTELEMETRY_INSTALLED:
            return await call_next(cmd)

        tracer = get_tracer()
        destination = _extract_destination(cmd)
        span_name = f"{destination} publish" if destination else "faststream publish"
        with tracer.start_as_current_span(
            span_name,
            kind=SpanKind.PRODUCER,
        ) as span:
            self._set_message_attrs(span, cmd, operation="publish")
            # Inject the *current* context (which is now the producer span)
            # into the outgoing message headers so consumers see this span
            # as the parent. Mutating ``cmd.headers`` in place is the
            # documented FastStream extension point.
            headers = cmd.headers if cmd.headers is not None else {}
            propagate.inject(carrier=headers)
            cmd.headers = headers
            try:
                return await call_next(cmd)
            except BaseException as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: "TracebackType | None" = None,
    ) -> bool | None:
        return None

    @staticmethod
    def _set_message_attrs(span: Any, obj: Any, *, operation: str) -> None:
        if not OPENTELEMETRY_INSTALLED:
            return
        span.set_attribute("messaging.system", "faststream")
        span.set_attribute("messaging.operation", operation)
        destination = _extract_destination(obj)
        if destination:
            span.set_attribute("messaging.destination.name", destination)
        message_id = getattr(obj, "correlation_id", None) or getattr(
            obj,
            "message_id",
            None,
        )
        if message_id:
            span.set_attribute("messaging.message.id", str(message_id))


# ----- @monitored: per-handler opt-in instrumentation ---------------------


@overload
def monitored(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]: ...


@overload
def monitored(
    *,
    span_name: str | None = ...,
    attributes: dict[str, Any] | None = ...,
    record_payload: bool = ...,
    record_payload_max_bytes: int = ...,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]: ...


def monitored(
    fn: Callable[P, Awaitable[R]] | None = None,
    *,
    span_name: str | None = None,
    attributes: dict[str, Any] | None = None,
    record_payload: bool = False,
    record_payload_max_bytes: int = _DEFAULT_PAYLOAD_TRUNCATION,
) -> (
    Callable[P, Awaitable[R]]
    | Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]
):
    """Opt-in per-handler instrumentation: OTel span + structured log + timing.

    Complements broker-wide :class:`OtelMiddleware` / ``LoggingMiddleware`` --
    use this decorator on individual subscribers when you only want
    instrumentation for a subset of handlers, or want to attach
    handler-specific attributes that don't belong on every consume.

    Args:
        fn: The handler function (only populated for the bare ``@monitored``
            form; ``None`` when used as ``@monitored(...)``).
        span_name: Span name. Defaults to the wrapped function's ``__qualname__``.
        attributes: Extra OTel attributes recorded on the span and emitted in
            the structured log line's ``extra=``. Use this for handler-scoped
            tags ("tier": "critical", "team": "billing").
        record_payload: If True, attach ``str(msg)[:record_payload_max_bytes]``
            as the ``messaging.message.body.preview`` span attribute. Off by
            default -- payloads frequently contain PII; opt in explicitly per
            handler.
        record_payload_max_bytes: Truncation cap for the recorded payload.
            Defaults to 1024 bytes (OTel messaging guidance).

    Returns:
        A wrapped coroutine function with the same signature as the original.

    Example::

        @broker.subscriber("orders")
        @monitored(
            span_name="process_order",
            attributes={"tier": "critical"},
            record_payload=True,
        )
        async def handle_order(msg: Order) -> None: ...

    Notes:
        * Span kind is INTERNAL: it's expected to nest under the CONSUMER
          span created by :class:`OtelMiddleware`. If middleware is absent the
          INTERNAL span is the root -- still emitted but less semantically
          rich than middleware + decorator together.
        * When ``opentelemetry`` is not installed the span work is a no-op
          (via :data:`_otel_typing.OPENTELEMETRY_INSTALLED`); the timing log
          line is always emitted.
    """

    def decorator(
        func: Callable[P, Awaitable[R]],
    ) -> Callable[P, Awaitable[R]]:
        resolved_name = span_name or getattr(func, "__qualname__", repr(func))
        attrs = dict(attributes or {})

        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            tracer = get_tracer()
            start = time.monotonic()
            ok = True
            # First positional arg is conventionally the message; SAFE to be
            # absent (e.g. handler signatures with no args), in which case we
            # skip payload recording entirely.
            msg = args[0] if args else None

            with tracer.start_as_current_span(
                resolved_name,
                kind=SpanKind.INTERNAL,
            ) as span:
                if OPENTELEMETRY_INSTALLED:
                    for k, v in attrs.items():
                        span.set_attribute(k, v)
                    if record_payload and msg is not None:
                        _record_payload(span, msg, record_payload_max_bytes)
                try:
                    return await func(*args, **kwargs)
                except BaseException as exc:
                    ok = False
                    if OPENTELEMETRY_INSTALLED:
                        span.record_exception(exc)
                        span.set_status(Status(StatusCode.ERROR, str(exc)))
                    raise
                finally:
                    elapsed_ms = (time.monotonic() - start) * 1000.0
                    _log.info(
                        "handler %s ok=%s in %.2fms",
                        resolved_name,
                        ok,
                        elapsed_ms,
                        extra={
                            "handler": resolved_name,
                            "duration_ms": round(elapsed_ms, 3),
                            "ok": ok,
                            **attrs,
                        },
                    )

        return cast("Callable[P, Awaitable[R]]", wrapper)

    if fn is not None:
        # Bare ``@monitored`` (no parens) -- ``fn`` is the handler itself.
        return decorator(fn)
    return decorator


def _record_payload(span: Any, msg: Any, max_bytes: int) -> None:
    """Attach a truncated string preview of the message body to the span.

    Defensive: ``str(msg)`` can raise on weird objects; never fail the handler
    because we couldn't snapshot the payload. The truncation cap is in *str
    length* (not bytes); for OTel attributes this is close enough given
    most payloads are ASCII-ish JSON.
    """
    try:
        preview = str(msg)
    except Exception:
        return
    if len(preview) > max_bytes:
        preview = preview[:max_bytes] + "…"
    span.set_attribute("messaging.message.body.preview", preview)
