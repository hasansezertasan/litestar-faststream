"""Internal helpers for ``BrokerConfig``.

Kept separate from ``plugin.py`` so the plugin module stays focused on the
``BrokerConfig`` class itself. These helpers are used by the config and by
``info.py`` for rendering the ``faststream info`` CLI subcommand.
"""

from typing import Any

__all__ = ["LazyMock", "qualname"]


def qualname(fn: Any) -> str:
    """Best-effort ``__qualname__`` for a possibly-wrapped callable."""
    target = fn.__func__ if hasattr(fn, "__func__") else fn
    name = getattr(target, "__qualname__", None) or getattr(target, "__name__", None)
    return name if name is not None else repr(target)


class LazyMock:
    """Defer ``wrapper.mock`` until first use.

    FastStream >= 0.7.7 raises ``SetupError`` when ``HandlerCallWrapper.mock``
    is read outside a ``Test*Broker``, so it cannot be copied onto the user's
    function at app-init time. Resolving it on attribute access keeps
    ``fn.mock.assert_called_once()`` working inside a test broker.
    """

    def __init__(self, wrapper: Any) -> None:
        self._wrapper = wrapper

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapper.mock, name)
