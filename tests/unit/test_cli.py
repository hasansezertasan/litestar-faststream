import weakref
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import click
import click.testing
import pytest

from litestar_faststream.cli import register_broker_cli


def _make_plugin(name: str) -> MagicMock:
    plugin = MagicMock(name=f"plugin-{name}")
    plugin.name = name
    plugin.broker = MagicMock()
    plugin.broker.start = AsyncMock()
    plugin.broker.stop = AsyncMock()
    plugin.broker.connect = AsyncMock()
    plugin.broker.ping = AsyncMock(return_value=True)
    plugin._registered_subscribers = []
    plugin._registered_response_publishers = []
    plugin._pending_controller_subscribers = []
    return plugin


@pytest.fixture(autouse=True)
def _reset_cli_module_state(monkeypatch) -> None:
    """Reset the per-group CLI registry between tests."""
    from litestar_faststream import cli as cli_mod

    monkeypatch.setattr(cli_mod, "_REGISTRY", weakref.WeakKeyDictionary())


def test_register_creates_faststream_group_with_run_info_status() -> None:
    cli = click.Group("litestar")
    plugin = _make_plugin("rabbit")
    register_broker_cli(cli, plugin)
    assert "faststream" in cli.commands
    fs_grp = cast("click.Group", cli.commands["faststream"])
    assert {"run", "info", "status"}.issubset(fs_grp.commands.keys())


def test_status_table_reports_each_broker() -> None:
    cli = click.Group("litestar")
    rabbit = _make_plugin("rabbit")
    redis = _make_plugin("redis")
    redis.broker.ping = AsyncMock(return_value=False)
    register_broker_cli(cli, rabbit)
    register_broker_cli(cli, redis)

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["faststream", "status"])
    assert result.exit_code == 0, result.output
    assert "rabbit" in result.output
    assert "redis" in result.output
    assert "OK" in result.output
    assert "DOWN" in result.output
    rabbit.broker.connect.assert_awaited_once()
    rabbit.broker.stop.assert_awaited_once()


def test_status_json_emits_structured_rows() -> None:
    import json

    cli = click.Group("litestar")
    plugin = _make_plugin("rabbit")
    plugin._registered_subscribers = [("h", (), {})]
    register_broker_cli(cli, plugin)

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["faststream", "status", "--json"])
    assert result.exit_code == 0, result.output
    rows = json.loads(result.output)
    assert rows[0]["broker"] == "rabbit"
    assert rows[0]["connected"] is True
    assert rows[0]["subscribers"] == 1


def test_status_broker_filter() -> None:
    cli = click.Group("litestar")
    rabbit = _make_plugin("rabbit")
    redis = _make_plugin("redis")
    register_broker_cli(cli, rabbit)
    register_broker_cli(cli, redis)

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["faststream", "status", "--broker", "redis"])
    assert result.exit_code == 0
    assert "redis" in result.output
    assert "rabbit" not in result.output
    rabbit.broker.connect.assert_not_called()
    redis.broker.connect.assert_awaited_once()


def test_drain_invokes_run_loop_with_timeout(monkeypatch) -> None:
    cli = click.Group("litestar")
    plugin = _make_plugin("rabbit")
    register_broker_cli(cli, plugin)

    captured: dict[str, object] = {}

    async def fake_loop(plugins, *, timeout=None) -> int:
        captured["plugins"] = list(plugins)
        captured["timeout"] = timeout
        return 0

    from litestar_faststream import cli as cli_mod

    monkeypatch.setattr(cli_mod, "_run_brokers_until_signal", fake_loop)

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["faststream", "drain", "--timeout", "5"])
    assert result.exit_code == 0, result.output
    assert captured["timeout"] == pytest.approx(5.0)
    assert captured["plugins"] == [plugin]


def test_drain_nonzero_exit_when_deadline_exceeded(monkeypatch) -> None:
    cli = click.Group("litestar")
    plugin = _make_plugin("rabbit")
    register_broker_cli(cli, plugin)

    async def fake_loop(plugins, *, timeout=None) -> int:
        return 1

    from litestar_faststream import cli as cli_mod

    monkeypatch.setattr(cli_mod, "_run_brokers_until_signal", fake_loop)

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["faststream", "drain", "--timeout", "0.1"])
    assert result.exit_code != 0
    assert "timeout" in result.output.lower()


@pytest.mark.asyncio()
async def test_run_loop_bounds_shutdown_when_broker_hangs(monkeypatch) -> None:
    """A hanging ``broker.stop`` must yield to the --timeout deadline."""
    import asyncio

    plugin = _make_plugin("rabbit")

    async def fake_startup(broker, *, publish_only=False) -> None:
        return None

    async def slow_shutdown(broker) -> None:
        await asyncio.sleep(10)

    from litestar_faststream import cli as cli_mod
    from litestar_faststream import lifespan as lifespan_mod

    monkeypatch.setattr(lifespan_mod, "run_broker_lifecycle_startup", fake_startup)
    monkeypatch.setattr(lifespan_mod, "run_broker_lifecycle_shutdown", slow_shutdown)

    # Skip the SIGINT wait so the loop drives straight to the shutdown phase.
    original_wait = asyncio.Event.wait

    async def quick_wait(self) -> None:
        return None

    monkeypatch.setattr(asyncio.Event, "wait", quick_wait)
    try:
        exit_code = await cli_mod._run_brokers_until_signal([plugin], timeout=0.05)
    finally:
        monkeypatch.setattr(asyncio.Event, "wait", original_wait)
    assert exit_code == 1


def test_status_reports_ping_error() -> None:
    cli = click.Group("litestar")
    plugin = _make_plugin("rabbit")
    plugin.broker.ping = AsyncMock(side_effect=RuntimeError("boom"))
    register_broker_cli(cli, plugin)

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["faststream", "status"])
    assert result.exit_code == 0
    assert "ERROR" in result.output
    assert "RuntimeError: boom" in result.output


def test_run_starts_broker_and_returns(monkeypatch) -> None:
    cli = click.Group("litestar")
    plugin = _make_plugin("rabbit")
    register_broker_cli(cli, plugin)

    async def fake_loop(plugins, **_kw: object) -> None:
        for p in plugins:
            await p.broker.start()
            await p.broker.stop()

    from litestar_faststream import cli as cli_mod

    monkeypatch.setattr(cli_mod, "_run_brokers_until_signal", fake_loop)

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["faststream", "run", "--broker", "rabbit"])
    assert result.exit_code == 0
    plugin.broker.start.assert_awaited_once()
    plugin.broker.stop.assert_awaited_once()
