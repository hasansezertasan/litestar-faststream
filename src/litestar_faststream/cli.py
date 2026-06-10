"""Litestar CLI subcommands for FastStream BrokerConfig: run / info / status / drain."""

import asyncio
import contextlib
import json as _json
import logging
import signal
import weakref
from collections.abc import Sequence
from itertools import starmap
from typing import Any

import click

logger = logging.getLogger(__name__)

# Per-cli-group state. Multiple Litestar apps in one process get isolated
# registries thanks to ``WeakKeyDictionary`` keyed on the click ``Group``.
#
# Lifetime note: this dict is module-global, so any ``click.Group`` whose
# lifetime is extended past one app build (e.g. a ``@pytest.fixture(scope=
# "module")`` that holds the Group) will keep its state alive for every
# subsequent ``register_broker_cli`` call against that Group. Tests today
# patch ``_REGISTRY`` per case via ``monkeypatch.setattr`` which sidesteps
# the issue. Production callers build a fresh ``click.Group`` per Litestar
# app, so state isolation holds.
_REGISTRY: weakref.WeakKeyDictionary[click.Group, dict[str, Any]] = (
    weakref.WeakKeyDictionary()
)


def register_broker_cli(cli: click.Group, plugin: Any) -> None:
    state = _REGISTRY.setdefault(cli, {"plugins": [], "installed": False})
    state["plugins"].append(plugin)

    if state["installed"]:
        return

    @cli.group(name="faststream")
    def faststream_group() -> None:
        """FastStream subcommands."""

    @faststream_group.command(name="run")
    @click.option(
        "--broker",
        "plugin_name",
        default=None,
        help="Run only the named broker.",
    )
    def run(plugin_name: str | None) -> None:
        plugins = _select(state["plugins"], plugin_name)
        asyncio.run(_run_brokers_until_signal(plugins))

    @faststream_group.command(name="info")
    @click.option("--broker", "plugin_name", default=None)
    def info(plugin_name: str | None) -> None:
        from .info import render_plugin_info

        plugins = _select(state["plugins"], plugin_name)
        for p in plugins:
            click.echo(render_plugin_info(p), nl=False)

    @faststream_group.command(name="status")
    @click.option(
        "--broker",
        "plugin_name",
        default=None,
        help="Probe only the named broker.",
    )
    @click.option(
        "--watch",
        is_flag=True,
        default=False,
        help="Refresh every --interval seconds until interrupted.",
    )
    @click.option(
        "--interval",
        type=float,
        default=2.0,
        show_default=True,
        help="Refresh interval (seconds) when --watch is set.",
    )
    @click.option(
        "--timeout",
        type=float,
        default=2.0,
        show_default=True,
        help="Per-broker ping timeout (seconds).",
    )
    @click.option(
        "--json",
        "as_json",
        is_flag=True,
        default=False,
        help="Emit JSON instead of a table.",
    )
    def status(
        plugin_name: str | None,
        watch: bool,  # noqa: FBT001 - click options bind by position
        interval: float,
        timeout: float,
        as_json: bool,  # noqa: FBT001 - click options bind by position
    ) -> None:
        plugins = _select(state["plugins"], plugin_name)
        asyncio.run(
            _status_loop(
                plugins,
                watch=watch,
                interval=interval,
                timeout=timeout,
                as_json=as_json,
            ),
        )

    @faststream_group.command(name="drain")
    @click.option(
        "--broker",
        "plugin_name",
        default=None,
        help="Drain only the named broker.",
    )
    @click.option(
        "--timeout",
        type=float,
        default=30.0,
        show_default=True,
        help="Maximum seconds to await in-flight messages before forcing exit.",
    )
    def drain(plugin_name: str | None, timeout: float) -> None:
        """Start brokers, wait for signal, then drain with a bounded deadline.

        Companion to ``run`` for k8s/SIGTERM scenarios: ``run`` waits for
        graceful shutdown indefinitely; ``drain`` caps the wait at --timeout
        so the process exits even if a handler hangs.

        Raises:
            click.ClickException: When the deadline is exceeded (so the
                process exits with a non-zero status).
        """
        plugins = _select(state["plugins"], plugin_name)
        exit_code = asyncio.run(_run_brokers_until_signal(plugins, timeout=timeout))
        if exit_code:
            msg = (
                f"drain exceeded --timeout={timeout}s; some in-flight messages "
                f"may not have completed"
            )
            raise click.ClickException(
                msg,
            )

    state["installed"] = True


def _select(registered: Sequence[Any], plugin_name: str | None) -> Sequence[Any]:
    if plugin_name is None:
        return list(registered)
    matching = [p for p in registered if p.name == plugin_name]
    if not matching:
        msg = f"No BrokerConfig named {plugin_name!r}"
        raise click.ClickException(msg)
    return matching


async def _run_brokers_until_signal(
    plugins: Sequence[Any],
    *,
    timeout: float | None = None,
) -> int:
    # Re-apply Litestar's logging config so worker-process formatting matches
    # the HTTP server (mirrors litestar-saq/cli.py:355-373). Apply for every
    # registered broker; with multiple BrokerConfigs the last call wins,
    # matching how standard-lib logging is configured app-wide.
    for p in plugins:
        cfg = getattr(p, "_logging_config", None)
        if cfg is None:
            continue
        try:
            cfg.configure()
        except Exception:
            logger.exception("Failed to apply logging_config from plugin %s", p.name)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal_handler() -> None:
        stop_event.set()

    # ``loop.add_signal_handler`` raises ``NotImplementedError`` on Windows
    # (the ProactorEventLoop does not support it). The suppression keeps
    # ``run`` usable there, but Ctrl-C will deliver ``KeyboardInterrupt``
    # straight through ``asyncio.run`` and skip the orderly broker drain
    # in the ``finally`` block below. Cross-platform orderly shutdown on
    # Windows would need a ``signal.signal`` fallback plus a polling loop
    # instead of ``stop_event.wait()``; that gap is documented rather than
    # worked around so behavior is predictable on the supported platforms.
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal_handler)

    from .lifespan import run_broker_lifecycle_shutdown, run_broker_lifecycle_startup

    started: list[Any] = []
    exceeded_deadline = False
    try:
        for p in plugins:
            await run_broker_lifecycle_startup(p.broker)
            started.append(p)
        await stop_event.wait()
    finally:
        # ``broker.stop()`` performs FastStream's graceful shutdown: it stops
        # accepting new messages, awaits in-flight handlers, then closes the
        # connection. When ``timeout`` is set (``drain`` subcommand), we cap
        # the total grace window across all brokers; the asyncio.wait_for
        # cancel propagates into the broker's stop coroutine, which then
        # tears down without waiting further.
        shutdown_tasks = [run_broker_lifecycle_shutdown(p.broker) for p in started]
        if shutdown_tasks:
            if timeout is None:
                for task in shutdown_tasks:
                    await task
            else:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*shutdown_tasks),
                        timeout=timeout,
                    )
                except asyncio.TimeoutError:
                    exceeded_deadline = True
                    logger.warning(
                        "graceful drain timed out after %.1fs; some in-flight "
                        "messages may have been interrupted",
                        timeout,
                    )
    return 1 if exceeded_deadline else 0


async def _probe_broker(plugin: Any, timeout: float) -> dict[str, Any]:
    """Connect, ping, and report a single broker's reachability.

    Uses ``BrokerUsecase.connect()`` + ``ping()`` (uniform across all FastStream
    brokers). The broker is closed again afterward so ``status`` never leaves
    a live connection behind -- it's a one-shot probe, not a worker.
    """
    broker = plugin.broker
    sub_count = len(getattr(plugin, "_registered_subscribers", []) or []) + len(
        getattr(plugin, "_pending_controller_subscribers", []) or [],
    )
    pub_count = len(getattr(plugin, "_registered_response_publishers", []) or [])
    result: dict[str, Any] = {
        "broker": plugin.name,
        "class": type(broker).__name__,
        "connected": False,
        "error": None,
        "subscribers": sub_count,
        "publishers": pub_count,
    }
    try:
        await broker.connect()
        result["connected"] = await broker.ping(timeout=timeout)
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
    return result


def _render_status_table(rows: Sequence[dict[str, Any]]) -> str:
    headers = ("BROKER", "CLASS", "STATE", "SUBS", "PUBS", "DETAIL")
    body: list[tuple[str, ...]] = []
    for row in rows:
        if row["error"] is not None:
            state = "ERROR"
            detail = row["error"]
        elif row["connected"]:
            state = "OK"
            detail = ""
        else:
            state = "DOWN"
            detail = "ping returned False"
        body.append((
            row["broker"],
            row["class"],
            state,
            str(row["subscribers"]),
            str(row["publishers"]),
            detail,
        ))
    widths = [max(len(h), *(len(r[i]) for r in body)) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    lines = [fmt.format(*headers), fmt.format(*("-" * w for w in widths))]
    lines.extend(starmap(fmt.format, body))
    return "\n".join(lines) + "\n"


async def _status_loop(
    plugins: Sequence[Any],
    *,
    watch: bool,
    interval: float,
    timeout: float,
    as_json: bool,
) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal_handler() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal_handler)

    while True:
        rows = await asyncio.gather(*(_probe_broker(p, timeout) for p in plugins))
        if as_json:
            click.echo(_json.dumps(list(rows), indent=2))
        else:
            if watch:
                # ANSI clear-screen + cursor-home for live refresh. Skipped on
                # the one-shot path so output stays grep/diff-friendly.
                click.echo("\x1b[2J\x1b[H", nl=False)
            click.echo(_render_status_table(rows), nl=False)
        if not watch:
            return
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue
        return
