"""Optional-import surface for OpenTelemetry.

The rest of the package imports from this module unconditionally:

* When ``opentelemetry`` is installed, the real symbols are re-exported and
  :data:`OPENTELEMETRY_INSTALLED` is ``True``.
* Otherwise no-op stubs are provided so ``OtelMiddleware`` can still be
  instantiated and called -- it simply becomes a pass-through.

Uses ``TYPE_CHECKING`` so static analyzers always see the real OpenTelemetry
types regardless of whether the optional dependency is installed in the
local environment. At runtime, the ``try`` branch swaps in stubs when the
import fails.
"""

from typing import TYPE_CHECKING, Any

from typing_extensions import Self

__all__ = (
    "OPENTELEMETRY_INSTALLED",
    "Span",
    "SpanKind",
    "Status",
    "StatusCode",
    "Tracer",
    "propagate",
    "trace",
)


if TYPE_CHECKING:
    from opentelemetry import propagate, trace
    from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer

    OPENTELEMETRY_INSTALLED = True
else:
    try:
        from opentelemetry import propagate, trace
        from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer

        OPENTELEMETRY_INSTALLED = True
    except ImportError:  # pragma: no cover - exercised in CI matrix without otel
        OPENTELEMETRY_INSTALLED = False

        class _Stub:
            """No-op stand-in covering every method we touch on OTel objects.

            Returning ``self`` from ``__call__`` lets the same instance double
            as a tracer (``trace.get_tracer(...)``) AND a context manager
            (``with tracer.start_as_current_span(...) as span:``). Cheap, ugly,
            works.
            """

            def __getattr__(self, _name: str) -> "_Stub":
                return self

            def __call__(self, *_args: Any, **_kwargs: Any) -> "_Stub":
                return self

            def __enter__(self) -> Self:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

        _STUB = _Stub()
        trace = _STUB
        propagate = _STUB
        Span = _Stub
        SpanKind = _Stub
        Status = _Stub
        StatusCode = _Stub
        Tracer = _Stub
