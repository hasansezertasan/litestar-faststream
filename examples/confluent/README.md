# Litestar + FastStream — Kafka (Confluent backend)

Identical wire protocol to `examples/kafka`, but uses the librdkafka-based
client from `faststream.confluent` instead of the pure-Python `aiokafka`
backend used by `faststream.kafka`. Pick this when you want librdkafka's
producer batching/feature set; pick the `kafka` example for a pure-Python
client.

Run Kafka locally:

```bash
docker compose -f examples/confluent/docker-compose.yml up -d
```

## Two-process

```bash
uv run litestar --app examples.confluent.app:app run
uv run litestar --app examples.confluent.app:app faststream run
```

## Single process

```bash
uv run litestar --app examples.confluent.app:app run
```

## Tests

```bash
uv run pytest examples/confluent/testing.py
```

## What this demonstrates

* `Controller` mixing HTTP siblings with a stream handler.
* Bridging an HTTP POST to a Kafka topic by injecting `kafka: KafkaBroker` into the route handler and calling `kafka.publish(...)` directly (Litestar DI).
* `@publisher` wrapping the subscriber's return value (FastStream-native `@broker.publisher` semantics).
* `after_startup` lifecycle hook.
* AsyncAPI docs at `/asyncapi`.
* **DI in FastStream subscribers** via `faststream.confluent.annotations` —
  `KafkaMessage` for the raw incoming message and `KafkaProducer` for the raw
  `AsyncConfluentFastProducer`.
* **DI in Litestar routes** — `GET /orders/stats` takes `kafka: KafkaBroker`;
  the broker is registered under `BrokerConfig.name` (defaults to `"kafka"`
  because the class is also called `KafkaBroker`).

## Live broker smoke

```bash
docker compose -f examples/confluent/docker-compose.yml up -d
uv run litestar --app examples.confluent.app:app run
uv run litestar --app examples.confluent.app:app faststream run
```

```bash
curl -X POST localhost:8000/orders -H 'content-type: application/json' \
     -d '{"user_id": 1, "item": "tea"}'
```

Tear down:

```bash
docker compose -f examples/confluent/docker-compose.yml down
```

Scripted variant:

```bash
bash examples/confluent/smoke.sh
```
