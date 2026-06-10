# Litestar + FastStream — Kafka

Run Kafka locally:

```bash
docker compose -f examples/kafka/docker-compose.yml up -d
```

## Two-process

```bash
uv run litestar --app examples.kafka.app:app run
uv run litestar --app examples.kafka.app:app faststream run
```

## Single process

```bash
uv run litestar --app examples.kafka.app:app run
```

## Tests

```bash
uv run pytest examples/kafka/testing.py
```

## What this demonstrates

* `Controller` mixing HTTP siblings with a stream handler.
* Bridging an HTTP POST to a Kafka topic by injecting `kafka: KafkaBroker` into the route handler and calling `kafka.publish(...)` directly (Litestar DI).
* `@publisher` wrapping the subscriber's return value (FastStream-native `@broker.publisher` semantics).
* `after_startup` lifecycle hook.
* AsyncAPI docs at `/asyncapi`.
* **DI in FastStream subscribers** via `faststream.kafka.annotations` —
  `KafkaMessage` for the raw incoming message, `Consumer` for the underlying
  `AIOKafkaConsumer`, and `KafkaProducer` for the raw producer.
* **DI in Litestar routes** — `GET /orders/stats` takes `kafka: KafkaBroker`;
  the broker is registered under `BrokerConfig.name` (defaults to `"kafka"`)
  and its class is exposed via `signature_namespace`.

## Live broker smoke

```bash
docker compose -f examples/kafka/docker-compose.yml up -d
```

Then in two terminals:

```bash
uv run litestar --app examples.kafka.app:app run
uv run litestar --app examples.kafka.app:app faststream run
```

Hit the HTTP endpoint:

```bash
curl -X POST localhost:8000/orders -H 'content-type: application/json' \
     -d '{"user_id": 1, "item": "tea"}'
```

Check AsyncAPI viewer at http://localhost:8000/asyncapi.

Tear down:

```bash
docker compose -f examples/kafka/docker-compose.yml down
```

A scripted variant lives in `smoke.sh`:

```bash
bash examples/kafka/smoke.sh
```
