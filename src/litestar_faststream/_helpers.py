"""Internal helpers for ``BrokerConfig``.

Kept separate from ``plugin.py`` so the plugin module stays focused on the
``BrokerConfig`` class itself. These helpers are used by the config and by
``info.py`` for rendering the ``faststream info`` CLI subcommand.
"""

from typing import Any


def qualname(fn: Any) -> str:
    """Best-effort ``__qualname__`` for a possibly-wrapped callable."""
    target = fn.__func__ if hasattr(fn, "__func__") else fn
    name = getattr(target, "__qualname__", None) or getattr(target, "__name__", None)
    return name if name is not None else repr(target)
