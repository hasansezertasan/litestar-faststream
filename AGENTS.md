# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A Litestar plugin that integrates [FastStream](https://faststream.airt.ai/) brokers (RabbitMQ, Kafka, NATS, Redis) into a Litestar application — managing broker lifecycle, dependency injection, and (optionally) AsyncAPI schema exposure.

The repository is currently empty: there is no source code, no `pyproject.toml`, no tests. Treat any work here as greenfield.

## Development approach

This project follows **Document-Driven Development** (see user global `CLAUDE.md`):

1. Write/update docs first — README, architecture, public API surface, configuration reference.
2. Get the documented design reviewed before implementing.
3. Build code to match the documented contract.

Do not add implementation code without a corresponding documented design. If a design question is unresolved, surface it rather than guessing.

## Stack conventions

- Python only, managed with `uv` (`uv init`, `uv add`, `uv run`, `uv sync`). Do not use `pip`, `poetry`, or `hatch` invocations.
- Type hints required on all public functions.
- PEP 8 / formatter-enforced style.
- Prefer the standard library; reach for third-party packages only when clearly justified.
- Runtime dependencies are expected to be `litestar` and `faststream` (with broker extras pulled in by the consumer, not pinned here).

## Plugin architecture (intended)

When implementation begins, the natural shape is:

- A plugin class (likely `FastStreamPlugin`) implementing Litestar's `InitPluginProtocol`, configured with a FastStream `Broker` (and optionally a `FastStream` app instance).
- `on_app_init` wires:
  - `on_startup` → `broker.start()`
  - `on_shutdown` → `broker.close()`
  - A DI provider exposing the broker (and/or `FastStream` app) to route handlers.
- Optional: mount AsyncAPI docs as a Litestar route group.

Confirm this shape against current Litestar and FastStream docs before coding — both libraries evolve. Use the `context7` MCP server (or the `litestar-plugins` / FastStream skills) for current API surface; do not rely on memory.

## Git conventions

- Commits: Conventional Commits v1.0.0
- Branches: Conventional Branch
- PR titles: Conventional Pull Request action format

## Not yet defined

There is no build, lint, or test command yet — `pyproject.toml` does not exist. When scaffolding, propose the toolchain (e.g. `ruff`, `pytest`, `mypy`/`pyright`) in docs first and get sign-off before adding it.
