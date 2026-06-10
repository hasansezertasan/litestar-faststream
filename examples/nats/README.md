# Litestar + FastStream — NATS

Run NATS locally:

```bash
docker compose -f examples/nats/docker-compose.yml up -d
```

## Two-process

```bash
uv run litestar --app examples.nats.app:app run
uv run litestar --app examples.nats.app:app faststream run
```

## Single process

```bash
uv run litestar --app examples.nats.app:app run
```

## Tests

```bash
uv run pytest examples/nats/testing.py
```

## What this demonstrates

* `Controller` mixing HTTP siblings with a stream handler.
* Bridging an HTTP POST to a NATS subject by injecting `nats: NatsBroker` into the route handler and calling `nats.publish(...)` directly (Litestar DI).
* `@publisher` wrapping the subscriber's return value (FastStream-native `@broker.publisher` semantics).
* `after_startup` lifecycle hook.
* AsyncAPI docs at `/asyncapi`.
* **DI in FastStream subscribers** via `faststream.nats.annotations` —
  `NatsMessage` for the raw incoming message and `Client` for the raw
  `nats.aio.client.Client`. JetStream / KV / object-store variants
  (`JsClient`, `NatsKvMessage`, `ObjectStorage`) are available when their
  respective subscriber types are used.
* **DI in Litestar routes** — `GET /orders/stats` takes `nats: NatsBroker`;
  the broker is registered under `BrokerConfig.name` (defaults to `"nats"`).

## Live broker smoke

```bash
docker compose -f examples/nats/docker-compose.yml up -d
uv run litestar --app examples.nats.app:app run
uv run litestar --app examples.nats.app:app faststream run
```

```bash
curl -X POST localhost:8000/orders -H 'content-type: application/json' \
     -d '{"user_id": 1, "item": "tea"}'
```

Tear down:

```bash
docker compose -f examples/nats/docker-compose.yml down
```

Scripted variant:

```bash
bash examples/nats/smoke.sh
```
