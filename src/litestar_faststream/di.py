"""DI bridge primitives between Litestar ``Provide`` and FastStream brokers.

Two flavours live here:

* :func:`build_broker_provide` — one ``Provide`` per broker, registered under
  the ``BrokerConfig.name`` so handlers can declare a typed parameter for a
  specific broker (e.g. ``rabbit: RabbitBroker``).

* :class:`Brokers` + :func:`build_brokers_provide` — a single aggregate
  ``Provide`` exposed under the literal key ``"brokers"`` so handlers that
  choose a broker at runtime can write ``brokers.get("rabbit").publish(...)``.
  Mirrors ``litestar-saq``'s ``TaskQueues`` pattern.
"""

from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any, TypeVar, overload

from litestar.di import Provide

from .exceptions import BrokerNotRegisteredError

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase

__all__ = (
    "Brokers",
    "build_broker_provide",
    "build_brokers_provide",
)

_T = TypeVar("_T")


class _Missing:
    """Private sentinel for ``Brokers.get`` default-arg discrimination.

    Using a typed singleton instead of ``Ellipsis`` lets the ``@overload``
    resolver distinguish the "no default → may raise" call shape from the
    "default supplied → returns default" call shape.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return "<MISSING>"


_MISSING = _Missing()


def build_broker_provide(broker: Any) -> Provide:
    def factory() -> Any:
        return broker

    return Provide(factory, sync_to_thread=False)


class Brokers(Mapping[str, "BrokerUsecase[Any, Any]"]):
    """Read-only registry of brokers keyed by ``BrokerConfig.name``.

    Implements :class:`collections.abc.Mapping`, so standard ``in``, ``len``,
    iteration, ``.keys()`` / ``.items()`` / ``.values()`` all work without
    extra surface. The only addition is :meth:`get` raising
    :class:`BrokerNotRegisteredError` (instead of returning ``None``) so
    typos surface immediately rather than as ``AttributeError`` on the next
    line. The default-arg form of :meth:`get` is preserved for callers that
    want the ``Mapping.get`` semantics explicitly.
    """

    __slots__ = ("_items",)

    def __init__(self, items: "Mapping[str, BrokerUsecase[Any, Any]]") -> None:
        # Copy into a plain dict so callers cannot mutate the registry
        # through the original mapping reference.
        self._items: dict[str, BrokerUsecase[Any, Any]] = dict(items)

    def __getitem__(self, name: str) -> "BrokerUsecase[Any, Any]":
        """Return broker ``name`` or raise :class:`BrokerNotRegisteredError`."""
        try:
            return self._items[name]
        except KeyError:
            raise self._missing(name) from None

    def __contains__(self, name: object) -> bool:
        """Return True if a broker with that name is registered."""
        return name in self._items

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered broker names."""
        return iter(self._items)

    def __len__(self) -> int:
        """Number of registered brokers."""
        return len(self._items)

    def __repr__(self) -> str:
        """Show sorted broker names for stable debugging output."""
        return f"Brokers({sorted(self._items)!r})"

    @overload
    def get(self, name: object) -> "BrokerUsecase[Any, Any]": ...
    @overload
    def get(
        self,
        name: object,
        default: "BrokerUsecase[Any, Any] | _T",
    ) -> "BrokerUsecase[Any, Any] | _T": ...
    def get(
        self,
        name: object,
        default: "Any" = _MISSING,
    ) -> "Any":
        """Return broker ``name`` or raise :class:`BrokerNotRegisteredError`.

        ``name`` is typed as ``object`` to stay compatible with
        :meth:`Mapping.get` (whose key parameter is ``object``); registered
        keys are always strings, so a non-string ``name`` simply misses.

        ``default`` keeps :class:`Mapping.get` semantics: pass any value
        (including ``None``) to fall back instead of raising. Overloads
        narrow the return type so callers using the default-arg form get
        ``BrokerUsecase | T`` rather than a lie.
        """
        if isinstance(name, str) and name in self._items:
            return self._items[name]
        if default is not _MISSING:
            return default
        raise self._missing(name)

    def names(self) -> tuple[str, ...]:
        """Tuple of registered broker names (sorted, stable)."""
        return tuple(sorted(self._items))

    def _missing(self, name: object) -> BrokerNotRegisteredError:
        return BrokerNotRegisteredError(
            f"No broker named {name!r} in Brokers registry; registered: {self.names()}",
        )


def build_brokers_provide(registry: Brokers) -> Provide:
    def factory() -> Brokers:
        return registry

    return Provide(factory, sync_to_thread=False)
