# Litestar + FastStream — RabbitMQ

Run RabbitMQ locally (e.g. `docker run -p 5672:5672 rabbitmq:3`).

## Two-process

```bash
uv run litestar --app examples.rabbit.app:app run
uv run litestar --app examples.rabbit.app:app faststream run
```

## Single process

```bash
uv run litestar --app examples.rabbit.app:app run
```

## Tests

```bash
uv run pytest examples/rabbit/testing.py
```

## What this demonstrates

* `Controller` mixing HTTP siblings with a stream handler.
* Bridging an HTTP POST to the broker by injecting `rabbit: RabbitBroker` into the route handler and calling `rabbit.publish(...)` directly (Litestar DI).
* `@publisher` wrapping the subscriber's return value (FastStream-native `@broker.publisher` semantics).
* `after_startup` lifecycle hook.
* AsyncAPI docs at `/asyncapi`.
* **DI in FastStream subscribers** via `faststream.rabbit.annotations` —
  `RabbitMessage` for the raw incoming message, `Channel` / `Connection` for
  the underlying aio-pika `RobustChannel` and `RobustConnection`, and
  `RabbitProducer` for the raw producer.
* **DI in Litestar routes** — `GET /orders/stats` takes `rabbit: RabbitBroker`;
  the broker is registered under `BrokerConfig.name` (defaults to `"rabbit"`).

## Live broker smoke

Local RabbitMQ >= 3.13 has deprecated the `transient_nonexcl_queues` feature flag that
FastStream's Rabbit broker uses. Pin to 3.12 for now:

```bash
docker compose -f examples/rabbit/docker-compose.yml up -d
```

Then in two terminals:

```bash
# HTTP server
uv run litestar --app examples.rabbit.app:app run

# Broker worker
uv run litestar --app examples.rabbit.app:app faststream run
```

Hit the HTTP endpoint:

```bash
curl -X POST localhost:8000/orders -H 'content-type: application/json' \
     -d '{"user_id": 1, "item": "tea"}'
```

Check AsyncAPI viewer at http://localhost:8000/asyncapi.

Tear down:

```bash
docker compose -f examples/rabbit/docker-compose.yml down
```

A scripted variant lives in `smoke.sh`:

```bash
bash examples/rabbit/smoke.sh
```
