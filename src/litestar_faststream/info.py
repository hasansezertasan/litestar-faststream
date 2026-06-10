"""Pretty-print registration metadata for a ``BrokerConfig``.

Kept separate from ``cli.py`` so the formatting logic is unit-testable
without going through Click.
"""

from litestar_faststream.plugin import BrokerConfig

_QUEUE_COL_WIDTH = 20


def _pad(target: str) -> str:
    """Left-justify ``target`` to a fixed column width for tabular output."""
    return target.ljust(_QUEUE_COL_WIDTH)


def _target_label(value: object) -> str:
    """Prefer ``.name`` for queue/topic objects; fall back to ``str(value)``."""
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    return str(value)


def render_plugin_info(plugin: BrokerConfig) -> str:
    """Render a human-readable summary of what a ``BrokerConfig`` registered.

    Reads the ``_registered_subscribers`` / ``_registered_response_publishers``
    lists populated by ``BrokerConfig._apply_to_app_config``. Returns a single
    string ending with a newline. Format mirrors the table-style rendering used
    by other Litestar plugins so the output is greppable and aligns visually.
    """
    lines: list[str] = [f"Broker: {plugin.name}"]

    subs = getattr(plugin, "_registered_subscribers", []) or []
    if subs:
        lines.append("  Subscribers:")
        for handler_qualname, args, _kwargs in subs:
            queue = _target_label(args[0]) if args else "?"
            lines.append(f"    {_pad(queue)} -> {handler_qualname}")

    response_pubs = getattr(plugin, "_registered_response_publishers", []) or []
    if response_pubs:
        lines.append("  Publishers:")
        for handler_qualname, args, _kwargs in response_pubs:
            target = _target_label(args[0]) if args else "?"
            lines.append(f"    {_pad(target)} -> {handler_qualname}")

    return "\n".join(lines) + "\n"
