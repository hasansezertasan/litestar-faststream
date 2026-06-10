# litestar_faststream

[**Litestar**](https://litestar.dev/) integration for [**FastStream**](https://faststream.ag2.ai/latest/) message brokers.

Wraps any FastStream broker (RabbitMQ, Kafka, Confluent, NATS, Redis, MQTT) as a Litestar plugin: broker lifecycle is tied to the app lifespan, subscribers/publishers are declared with free decorators, the broker is exposed via Litestar's DI, AsyncAPI docs are mounted automatically, and a `litestar faststream run` CLI subcommand starts a broker-only worker.

> This package is the *Litestar* integration only. For the FastStream framework itself, see [ag2ai/faststream](https://github.com/ag2ai/faststream).

## When to use

* You are building a Litestar HTTP service that also needs to consume or publish broker messages.
* You want a single `litestar` CLI that can run either the HTTP server or a worker-only process for the broker.
* You want AsyncAPI docs served alongside Litestar's OpenAPI docs.

[![License](https://img.shields.io/github/license/hasansezertasan/litestar-faststream.svg)](./LICENSE)
[![Python versions](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue.svg)](./pyproject.toml)

---

## Installation

```sh
pip install litestar_faststream
```

You also need FastStream with the extra for your broker of choice:

```sh
pip install 'faststream[rabbit]'      # or kafka / confluent / nats / redis / mqtt
```

## Quickstart

```python
from litestar import Litestar

from faststream import Logger
from faststream.rabbit import RabbitBroker

from litestar_faststream import (
    BrokerConfig,
    FastStreamConfig,
    FastStreamPlugin,
    subscriber,
)

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


@subscriber("test-q")
async def handler(user_id: int, logger: Logger) -> str:
    logger.info(user_id)
    return f"{user_id} created"


app = Litestar(
    plugins=[
        FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)])),
    ],
)
```

### Run

Two-process layout — HTTP server and broker worker as separate processes:

```bash
# Terminal 1: HTTP server
uv run litestar run

# Terminal 2: broker worker
uv run litestar faststream run
```

Or single-process — `litestar run` starts both via the Litestar lifespan.

### What is happening

1. The broker instance is created at module level (FastStream-native).
2. A `FastStreamPlugin` wrapping `FastStreamConfig(brokers=[BrokerConfig(broker=broker)])` is added to `Litestar(plugins=[...])`. The plugin:
    * starts/stops every configured broker via the Litestar lifespan;
    * mounts a combined AsyncAPI HTML route at `FastStreamConfig.asyncapi_url` when set;
    * registers each broker as a Litestar dependency under the `BrokerConfig.name` (defaults to the broker class name lower-cased, e.g. `rabbit`);
    * walks `route_handlers` looking for `@subscriber` / `@publisher` markers.
3. Each marked function is bound to the broker exactly once. For methods on a `Controller`, the plugin binds the method to the *same* Controller instance Litestar uses for HTTP, so `self` is shared between HTTP and stream handlers (see [Semantics on Controller methods](#semantics-on-controller-methods)). Controller-level config (`path`, `dependencies`) still applies to HTTP siblings only, not to the FastStream subscriber.

A more complete demo (HTTP routes, controller-hosted subscriber, startup hook, HTTP-to-broker via DI) lives in [`examples/rabbit/app.py`](./examples/rabbit/app.py). The same `Orders` demo is mirrored per broker — [`kafka`](./examples/kafka/app.py), [`confluent`](./examples/confluent/app.py), [`nats`](./examples/nats/app.py), [`redis`](./examples/redis/app.py), [`mqtt`](./examples/mqtt/app.py) — so the only thing that changes between them is the broker import. For a multi-broker setup, see [`examples/multiple_plugins_host/app.py`](./examples/multiple_plugins_host/app.py) (Kafka + Redis on a single Litestar app).

## Features

- **`FastStreamPlugin`** — the single Litestar plugin. Owns the lifespan, DI registration, marker discovery, and combined AsyncAPI rendering for every broker it hosts.
- **`FastStreamConfig`** / **`BrokerConfig`** — pure config dataclasses. `FastStreamConfig.brokers` is a list of `BrokerConfig` entries; one `FastStreamPlugin` consumes them.
- **Free decorators** — `@subscriber(...)` and `@publisher(...)` mark handlers anywhere in your code; the plugin discovers them at app init.
- **AsyncAPI** — combined document served at `FastStreamConfig.asyncapi_url` when set.
- **CLI** — `litestar faststream run` starts a broker-only worker process (no HTTP). Pass `--broker <name>` to scope to one configured broker.
- **Lifespan hooks** — `plugin.after_startup(name)(fn)` and `plugin.on_broker_shutdown(name)(fn)`.

## Supported brokers

All brokers use the same `BrokerConfig`. Subscriber-side typed `Annotated` aliases (broker, message, raw client, etc.) come straight from FastStream — `litestar_faststream` adds no per-broker wrappers:

| Broker     | Typed aliases                                                                                          |
|------------|--------------------------------------------------------------------------------------------------------|
| RabbitMQ   | `from faststream.rabbit.annotations import RabbitBroker, RabbitMessage, RabbitProducer, Channel, Connection` |
| Kafka      | `from faststream.kafka.annotations import KafkaBroker, KafkaMessage, KafkaProducer, Consumer`          |
| Confluent  | `from faststream.confluent.annotations import KafkaBroker, KafkaMessage, KafkaProducer`                |
| NATS       | `from faststream.nats.annotations import NatsBroker, NatsMessage, Client, JsClient, ObjectStorage`     |
| Redis      | `from faststream.redis.annotations import RedisBroker, RedisChannelMessage, Redis, Pipeline`           |
| MQTT       | `from faststream.mqtt.annotations import MQTTBroker, MQTTMessage`                                      |

`faststream.confluent.KafkaBroker` collides on class name with `faststream.kafka.KafkaBroker`, so when using Confluent pass an explicit `BrokerConfig(broker=..., name="confluent")` to disambiguate DI / CLI keys.

> FastStream's MQTT annotations don't ship a raw-client alias; reach for `broker._connection` if you need the underlying `zmqtt.MQTTClient`.

---

# Guide

- [Controllers and free decorators](#controllers-and-free-decorators)
- [Publishers](#publishers)
- [Dependencies and DI bridge](#dependencies-and-di-bridge)
- [Lifespan and hooks](#lifespan-and-hooks)
- [AsyncAPI documentation](#asyncapi-documentation)
- [CLI](#cli)
- [Multi-broker apps](#multi-broker-apps)
- [Testing](#testing)
- [Migrating from the FastAPI integration](#migrating-from-the-fastapi-integration)

## Controllers and free decorators

The integration ships two free decorators usable independently of any broker instance:

* `subscriber(*args, plugin: str | None = None, **kwargs)` — marks a function as a subscriber for the FastStream broker the plugin owns.
* `publisher(*args, plugin: str | None = None, **kwargs)` — marks a function as a publisher; the function's return value is published to the named queue/topic (mirrors `@broker.publisher`).

> **`@subscriber` and `@publisher` mark broker handlers, not HTTP routes.** Applying either to a method that also carries an HTTP-verb decorator (`@get`, `@post`, …) raises at app init. HTTP handlers that need to publish should inject the broker via Litestar DI — see [Publishing from an HTTP handler](#publishing-from-an-http-handler).

Both stash metadata on the function (`__faststream_subscribers__`, `__faststream_publishers__`). When `FastStreamPlugin.on_app_init` runs, the plugin walks `app_config.route_handlers` (recursively, including nested `Router`s) and any `Controller` classes, picks up marked methods/functions, extracts the underlying callable via `__func__`, and registers it with `broker.subscriber(...)` / `broker.publisher(...)`.

The optional `plugin=` keyword routes a marker to exactly one `BrokerConfig` by `name`; see [Multi-broker apps](#multi-broker-apps).

```python
from litestar import Controller, Litestar, get

from faststream import Logger
from faststream.rabbit import RabbitBroker

from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin, subscriber

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


class HelloController(Controller):
    path = "/hello"

    @get("/")
    async def greet(self) -> dict:
        return {"hi": True}

    @subscriber("greet")
    async def on_greet(name: str, logger: Logger) -> None:
        logger.info("greet", extra={"name": name})


app = Litestar(
    plugins=[FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))],
    route_handlers=[HelloController],
)
```

### Semantics on Controller methods

Stream handlers on a `Controller` receive `self` bound to the **same singleton instance** Litestar uses for HTTP handlers in that class. State stored on `self` (caches, lazily-initialised resources, etc.) is visible to both the HTTP routes and the stream subscribers in the same Controller. For stream-only Controllers (no `@get`/`@post` siblings), the plugin creates a single instance and reuses it for the app's lifetime.

The binding is **deferred to lifespan startup**, not done at `on_app_init`. Discovery happens during `on_app_init` and records each Controller-bound marker in `_pending_controller_subscribers`; the actual `broker.subscriber(...)` registration runs in a `pre_startup` lifespan hook (`_bind_and_register_controllers`), just before `broker.connect()/start()`. This ordering matters:

* Litestar only builds `app.routes` after `on_app_init` returns; the singleton instances we need to bind to are not reachable until then.
* Registering with the broker after Litestar has finished route construction (but before `broker.start()` iterates its subscriber set) lets the bound methods participate in the broker's normal startup path.

Implications:

* `self` **is available** and points at the shared Controller singleton.
* Controller-level config (`path`, `dependencies`, `middleware`, `guards`) still applies to HTTP siblings only; it does **not** flow into the FastStream subscriber (those features are tied to Litestar's per-request scope).
* `staticmethod` and `classmethod` decorated handlers stay unbound — `self`/`cls` semantics follow standard Python.
* Module-level functions are equally supported and remain the simplest option when shared state isn't needed.

### Mixing with Tier 1 (broker-bound) decorators

`@broker.subscriber(...)` from native FastStream still works. The plugin's discovery introspects `broker._subscribers` by `(function identity, queue)` before registering a Tier-2 marker, so a function bound both ways is registered exactly once. When the broker subscriber does not expose a comparable queue hint, the plugin falls back to identity-only dedup.

### Registration sources

* Discovery scans `app_config.route_handlers` (Controllers, Routers, route handlers) for markers.
* `BrokerConfig(handlers=[fn1, fn2])` — explicit list of `@subscriber`/`@publisher`-marked callables. Each entry must carry a marker; a callable without one raises at startup.

## Publishers

`@publisher` mirrors FastStream's `@broker.publisher`: the decorated function's return value is published to the named queue/topic. Stack it on top of `@subscriber` to publish the subscriber's response, or use it on its own when something else (an `after_startup` hook, a separate broker call) drives the function.

```python
from litestar import Litestar

from faststream import Logger
from faststream.rabbit import RabbitBroker

from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin, publisher, subscriber

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


@subscriber("orders.new")
@publisher("orders.processed")
async def on_new_order(payload: dict, logger: Logger) -> dict:
    logger.info("processing", extra={"payload": payload})
    return {**payload, "ok": True}


app = Litestar(
    plugins=[FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker, handlers=[on_new_order])]))],
)
```

### Publishing from an HTTP handler

`@publisher` is for broker handlers. To publish from inside an HTTP route handler, **inject the broker via Litestar DI** and call `broker.publish(...)` yourself. The integration registers each broker as a Litestar dependency under the `BrokerConfig.name` (defaults to the broker class lower-cased, e.g. `rabbit`) and adds the broker class to `signature_namespace`, so no extra imports are needed in handler modules:

```python
from dataclasses import dataclass

from litestar import Litestar, post

from faststream.rabbit import RabbitBroker

from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin


@dataclass
class Email:
    subject: str
    receiver: str
    body: str


broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


@post("/send-email")
async def send_email_endpoint(data: Email, rabbit: RabbitBroker) -> dict:
    await rabbit.publish(data, queue="send-email")
    return {"queued": True}


app = Litestar(
    plugins=[FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))],
    route_handlers=[send_email_endpoint],
)
```

This is the same DI pattern documented under [Litestar handlers receive broker / logger](#litestar-handlers-receive-broker--logger). You stay in full control of ordering (publish before vs. after the work), error handling (catch broker errors and respond as you choose), and which queue/topic each request lands on. The `@subscriber` on the worker side is unchanged — it's the receiving end of the same queue.

> **`@subscriber` / `@publisher` cannot decorate an HTTP route handler.** A method that already carries `@get` / `@post` / `@put` / `@delete` / `@patch` / `@route` (or sits inside an `HTTPRouteHandler`) is rejected at app init if it also has `@subscriber` or `@publisher` markers. Use DI instead — the example above.

## Dependencies and DI bridge

The integration bridges Litestar `Provide` and FastStream `FastDepends` in both directions.

### Litestar handlers receive broker / logger

`BrokerConfig` registers each broker in `app_config.dependencies` under `BrokerConfig.name` (defaults to the broker class name lower-cased — `"rabbit"`, `"kafka"`, …). HTTP handlers inject it by declaring a parameter of that name; the broker class is also added to `signature_namespace`, so the annotation resolves without an explicit import in handler modules.

```python
from litestar import Litestar, get

from faststream.rabbit import RabbitBroker

from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


@get("/queue-something")
async def trigger(rabbit: RabbitBroker) -> dict:
    await rabbit.publish({"hello": "world"}, queue="greetings")
    return {"published": True}


app = Litestar(
    plugins=[FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))],
    route_handlers=[trigger],
)
```

The literal key `"broker"` is intentionally **not** registered, to avoid collisions when multiple brokers coexist. Use the name-matching pattern above, or override `name=` on `BrokerConfig` for a deployment-specific key (`BrokerConfig(broker=..., name="orders")` → handler param `orders: RabbitBroker`).

### Loggers

Two patterns, both first-class:

* **HTTP handlers** — use Litestar's standard `request.logger`. No plugin work required.
* **Stream subscribers** — annotate a parameter with `Logger` (from `faststream`). It resolves to the broker's per-message logger via FastStream's context.

```python
from faststream import Logger

from litestar_faststream import subscriber

@subscriber("orders.new")
async def handle_order(payload: dict, logger: Logger) -> None:
    logger.info("got order", extra={"payload": payload})
```

### FastStream subscribers reading Litestar dependencies (follow-up)

A reverse-direction bridge (FastStream subscribers receiving Litestar `Provide`-defined dependencies) is tracked as a follow-up. v1 ships only the Litestar→FastStream direction described above. Subscribers receive FastStream-context dependencies (logger, broker, message) only.

### CLI worker process logging

`litestar faststream run` reuses `app_config.logging_config` (mirrors `litestar-saq/cli.py:355`). If `StructLoggingConfig` is in use, its `standard_lib_logging_config` is re-applied so module-level `logging.getLogger(__name__)` calls share handlers and formatters with the HTTP server output.

## Lifespan and hooks

`BrokerConfig` injects an async context manager into `app_config.lifespan` so the broker starts and stops alongside the Litestar app. Composition order:

```text
async with user_lifespan(app):              # outermost
    async with plugin_lifespan(app):        # per BrokerConfig
        await broker.connect()
        await broker.start()
        for hook in plugin._after_startup_hooks:
            await hook(app)
        yield
        for hook in plugin._on_broker_shutdown_hooks:
            await hook(app)
        await broker.stop()
```

User-supplied `app_config.on_startup` / `on_shutdown` callables run inside Litestar's own lifespan layer; they compose naturally with the plugin's CM.

### FastStream-style parity hooks

```python
from litestar import Litestar

from faststream.rabbit import RabbitBroker

from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
plugin = FastStreamPlugin(
    FastStreamConfig(brokers=[BrokerConfig(broker=broker, name="rabbit")]),
)


@plugin.after_startup("rabbit")
async def warm_up(app: Litestar) -> None:
    app.logger.info("broker ready")


@plugin.on_broker_shutdown("rabbit")
async def announce_shutdown(app: Litestar) -> None:
    app.logger.info("broker shutting down")


app = Litestar(plugins=[plugin])
```

`FastStreamPlugin.after_startup` and `on_broker_shutdown` take a broker name and return the actual decorator — the two-step shape is what disambiguates which broker the hook targets in a multi-broker app. For single-broker apps you can also decorate the `BrokerConfig` directly:

```python
cfg = BrokerConfig(broker=broker)


@cfg.after_startup
async def warm_up(app: Litestar) -> None: ...


plugin = FastStreamPlugin(FastStreamConfig(brokers=[cfg]))
```

* `fn(app)` for `after_startup` runs after `broker.start()`, before the app yields.
* `fn(app)` for `on_broker_shutdown` runs before `broker.stop()`.

Hook errors are logged but do not block lifespan teardown — `broker.stop()` always runs.

## Deployment shapes (`publish_only`)

`BrokerConfig(..., publish_only=True)` skips `broker.start()` during ASGI lifespan startup. The broker still calls `connect()`, so `broker.publish(...)` works from your route handlers — but no subscriber consume-loops are started in that process. Subscribers stay *registered* (so they remain visible in AsyncAPI), they just don't fire.

Three deployment shapes the same plugin config supports:

| Shape | Plugin config | How to launch |
|---|---|---|
| **A — Monolith.** One process: HTTP + publish + consume. | `BrokerConfig(broker)` (default) | `uvicorn app:app` or `litestar run`. |
| **B — Split.** Two processes from one codebase: HTTP publishes, worker consumes. | `BrokerConfig(broker, publish_only=True)` | API pod: `uvicorn app:app`. Worker pod: `litestar faststream run`. |
| **C — Publisher only.** One process, no subscribers exist anywhere. | `BrokerConfig(broker, publish_only=True)` | `uvicorn app:app`. |

`litestar faststream run` ignores `publish_only` on purpose — the CLI's whole job *is* running the broker, so it always calls `connect()+start()`. That's what makes shape B work without two app factories or an env-var branch in user code.

If you set `publish_only=True` and `@subscriber` handlers are present in the same process, the plugin logs a warning at startup (`N subscriber(s) registered but will not consume in this process`). This is intentional: silent skips become "why isn't my handler firing?" support tickets.

`publish_only` is set per-`BrokerConfig`, so a multi-broker app can be publish-only to Kafka while running a full Rabbit broker if that's the deployment shape you need.

See [`examples/publish_only/`](./examples/publish_only/) for a runnable scenario-B example.

## AsyncAPI documentation

`FastStreamConfig.asyncapi_url` mounts an HTML viewer + JSON/YAML schema endpoints for a combined AsyncAPI document covering every hosted broker. There is no per-broker URL — the document is one, even with N brokers (FastStream's `AsyncAPI(*brokers, ...)` factory accepts multiple brokers natively).

```python
FastStreamPlugin(
    FastStreamConfig(
        brokers=[BrokerConfig(broker=broker)],
        asyncapi_url="/asyncapi",
        asyncapi_include_in_schema=False,  # default — AsyncAPI URLs hidden from OpenAPI
        title="My Service",
        description="...",
        version="1.0.0",
        tags=[...],
    ),
)
```

Endpoints mounted (when `asyncapi_url` is set):

| URL | Content |
|---|---|
| `{asyncapi_url}` | HTML viewer |
| `{asyncapi_url}.json` | AsyncAPI JSON schema |
| `{asyncapi_url}.yaml` | AsyncAPI YAML schema |

Omit `asyncapi_url` (or leave it `None`) to disable AsyncAPI rendering entirely.

`asyncapi_include_in_schema` defaults to `False` because AsyncAPI HTML/JSON/YAML are meta-docs about the broker — they shouldn't pollute Litestar's OpenAPI document. Flip to `True` if you want the AsyncAPI URLs in your OpenAPI spec.

### Gating the AsyncAPI route

The AsyncAPI endpoints are public by default. To restrict them, use Litestar's standard mechanisms — middleware, `guards`, or a reverse-proxy ACL — scoped at the app or path level. The plugin does not expose its own auth knob; this matches [`litestar-asyncapi`](https://github.com/cofin/litestar-asyncapi) and keeps a single source of truth for auth in your Litestar config.

```python
from litestar import Litestar
from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException
from litestar.handlers.base import BaseRouteHandler


def asyncapi_guard(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    if connection.url.path.startswith("/asyncapi") and not connection.headers.get("x-internal"):
        raise NotAuthorizedException


app = Litestar(plugins=[FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))], guards=[asyncapi_guard])
```

Or disable the route entirely with `asyncapi_url=None` and serve the schema yourself.


## CLI

`FastStreamPlugin` extends Litestar's CLI via `CLIPluginProtocol` with a `faststream` subcommand group, enabling dual-process deployment (HTTP server + broker worker).

```bash
# Run HTTP server (and broker via Litestar lifespan)
uv run litestar run

# Run broker-only worker (no HTTP server)
uv run litestar faststream run                       # all configured brokers
uv run litestar faststream run --broker rabbit       # one specific broker by name

# Inspect registered subscribers / publishers
uv run litestar faststream info
```

### How `litestar faststream run` works

1. Builds the `AppConfig` (without binding an HTTP socket).
2. Reuses `app_config.logging_config`, re-applying its handlers so worker output matches HTTP server output (mirrors `litestar-saq/cli.py:355-373`).
3. For the matching broker(s), calls `await broker.start()`.
4. Installs SIGINT/SIGTERM handlers.
5. With multiple brokers and no `--broker`, starts all concurrently and shares one signal handler.
6. On shutdown signal, runs `on_broker_shutdown` hooks then `broker.stop()` for each broker.

### When to use this vs. `faststream run`

* `faststream run app:broker` — pure FastStream apps with no HTTP layer.
* `litestar faststream run` — Litestar apps where the broker config lives inside the Litestar `AppConfig` (plugin instance, dependencies, logging).

## Multi-broker apps

One `FastStreamPlugin` hosts every broker. Pass a list of `BrokerConfig` entries to `FastStreamConfig.brokers`; each entry owns one broker instance and one CLI name. The plugin serves a single combined AsyncAPI document at `FastStreamConfig.asyncapi_url`.

```python
from litestar import Litestar

from faststream.kafka import KafkaBroker
from faststream.redis import RedisBroker

from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin

kafka_broker = KafkaBroker("localhost:9092")
redis_broker = RedisBroker("redis://localhost:6379")

app = Litestar(
    plugins=[
        FastStreamPlugin(
            FastStreamConfig(
                brokers=[
                    BrokerConfig(broker=kafka_broker, name="kafka"),
                    BrokerConfig(broker=redis_broker, name="redis"),
                ],
                asyncapi_url="/asyncapi",
            ),
        ),
    ],
)
```

### Routing markers to a specific plugin

By default every `BrokerConfig` in the app claims every `@subscriber` / `@publisher` marker it finds — useful for single-broker apps. With multiple plugins, this means every marker binds to every broker. Tag the marker with `plugin=` to route it to one plugin only (matching `BrokerConfig.name`):

```python
from litestar_faststream import publisher, subscriber

@subscriber("orders.new", plugin="kafka")           # only the kafka BrokerConfig binds this
async def consume_order(payload: dict) -> None: ...

@publisher("orders.processed", plugin="kafka")      # publish to kafka only
@subscriber("orders.new", plugin="kafka")
async def process_order(payload: dict) -> dict: ...
```

Markers without `plugin=` keep the broadcast behavior. A marker tagged with a name no plugin claims is silently ignored — the integration logs a `WARNING` at app init listing the unmatched names so the typo is easy to spot. Pass `strict=True` to any `BrokerConfig` in the app to promote that warning to an `ImproperlyConfiguredException` and fail-fast at startup instead.

See [`examples/multiple_plugins_host/app.py`](./examples/multiple_plugins_host/app.py) for a Kafka + Redis end-to-end demo.

### Constraints

* **Broker name collision** — `FastStreamPlugin.__init__` raises `ImproperlyConfiguredException` if two `BrokerConfig` entries in the same `FastStreamConfig` share the same `name`. Default `name` is the broker class lower-case (`"rabbit"`, `"kafka"`).
* **DI** — each broker is registered under `BrokerConfig.name`. Handlers inject by parameter name (`rabbit: RabbitBroker`, `kafka: KafkaBroker`); the broker classes are added to `signature_namespace` so the annotations resolve without explicit imports.
* **Confluent name clash** — `faststream.confluent.KafkaBroker` shares a class name with `faststream.kafka.KafkaBroker`, so the default `name` would collide. Pass `name="confluent"` explicitly on the Confluent `BrokerConfig`.

## Testing

The integration ships no new test surface. Compose two existing primitives:

* `TestRabbitBroker(broker)` — FastStream's in-memory broker stand-in.
* `litestar.testing.AsyncTestClient(app)` — Litestar's HTTP test client (drives the lifespan).

```python
import pytest
from litestar import Litestar
from litestar.testing import AsyncTestClient

from faststream.rabbit import RabbitBroker, TestRabbitBroker

from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin, subscriber

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


@subscriber("ping")
async def on_ping(name: str) -> str:
    return f"hello {name}"


app = Litestar(
    plugins=[FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker, handlers=[on_ping])]))],
)


@pytest.mark.asyncio
async def test_ping_subscriber():
    async with TestRabbitBroker(broker), AsyncTestClient(app) as client:
        await broker.publish("Alice", queue="ping")
        on_ping.mock.assert_called_once_with("Alice")
```

### Pattern notes

* `AsyncTestClient(app)` runs the Litestar lifespan, which starts the (now in-memory) broker via the plugin.
* `subscriber.mock` (FastStream-native) returns the most recent call payload.
* When an HTTP handler publishes via injected broker, asserting on the downstream subscriber's `.mock` is the cleanest way to verify the call fired end-to-end.

## Migrating from the FastAPI integration

Side-by-side mapping for users porting an existing app.

| FastAPI integration | Litestar integration |
|---|---|
| `from faststream.rabbit.fastapi import RabbitRouter` | `from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin` |
| `RabbitRouter(url, schema_url="/asyncapi")` | `FastStreamConfig(brokers=[BrokerConfig(broker=RabbitBroker(url))], asyncapi_url="/asyncapi")` |
| `app.include_router(router)` | `Litestar(plugins=[FastStreamPlugin(FastStreamConfig(...))], ...)` |
| `@router.subscriber("q")` | `@broker.subscriber("q")` *or* `@subscriber("q")` (free) |
| `@router.publisher("q")` | `@broker.publisher("q")` *or* `@publisher("q")` (free) |
| `@router.after_startup(fn)` | `plugin.after_startup(name)(fn)` *(or `cfg.after_startup(fn)` on the `BrokerConfig` directly)* |
| `@router.on_broker_shutdown(fn)` | `plugin.on_broker_shutdown(name)(fn)` *(or `cfg.on_broker_shutdown(fn)`)* |
| `Logger` | `from faststream import Logger` |
| `RabbitBroker`, `RabbitMessage`, `RabbitProducer` | `from faststream.rabbit.annotations import RabbitBroker, RabbitMessage, RabbitProducer` |
| `Context` (FastDepends primitive) | `from faststream import Context` |

### Example diff

A typical FastStream + FastAPI app:

```python
from fastapi import FastAPI
from faststream.rabbit.fastapi import Logger, RabbitRouter

router = RabbitRouter("amqp://guest:guest@localhost:5672/")
app = FastAPI()

publisher = router.publisher("response-q")

@publisher
@router.subscriber("test-q")
async def handler(user_id: int, logger: Logger) -> str:
    logger.info(user_id)
    return f"{user_id} created"

app.include_router(router)
```

Idiomatic Litestar equivalent (see [`examples/rabbit/app.py`](./examples/rabbit/app.py)):

```python
from litestar import Litestar
from faststream import Logger
from faststream.rabbit import RabbitBroker

from litestar_faststream import BrokerConfig, FastStreamConfig, FastStreamPlugin

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

@broker.publisher("response-q")
@broker.subscriber("test-q")
async def handler(user_id: int, logger: Logger) -> str:
    logger.info(user_id)
    return f"{user_id} created"

app = Litestar(plugins=[FastStreamPlugin(FastStreamConfig(brokers=[BrokerConfig(broker=broker)]))])
```

The diff is small because broker-side decorators are unchanged; only the surrounding HTTP framework wrapper differs.

### What's new in the Litestar integration (no FastAPI equivalent)

These features exist in the Litestar integration only — there is nothing to map from FastAPI:

* **Free `@subscriber` / `@publisher` decorators on `Controller` methods** — discovery walks `route_handlers`, binds methods to Litestar's Controller singleton, and registers them with the broker. The FastAPI integration only supports the router-bound form. See [Controllers and free decorators](#controllers-and-free-decorators).
* **Multi-broker apps with `plugin=` marker filter** — multiple `BrokerConfig`s in one Litestar app, with markers optionally scoped to a specific plugin. See [Multi-broker apps](#multi-broker-apps).
* **`litestar faststream run` / `litestar faststream info` CLI subcommands** — start brokers without the HTTP server (dual-process deployment) or inspect the registered subscribers/publishers per plugin. See [CLI](#cli).
* **`strict=True` on `BrokerConfig`** — promote unknown `plugin=` marker filters from a startup warning to an init-time error.

### Things that did not carry over

* **FastDepends `Context` injection into Litestar HTTP handlers** is not provided. Use Litestar's own DI: declare a parameter named after the `BrokerConfig.name` (default `rabbit`, `kafka`, …) typed with the broker class.
* **HTTP-scoped Litestar dependencies inside FastStream subscribers** are not available — subscribers run without request scope. Use module-level state, the Controller singleton, or `app.state`.

## License

[LICENSE](./LICENSE).
