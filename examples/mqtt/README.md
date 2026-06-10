# Litestar + FastStream — MQTT

Run Mosquitto locally (config bundled, allows anonymous connections):

```bash
docker compose -f examples/mqtt/docker-compose.yml up -d
```

> MQTT topics use `/` as the level separator, not `.` — note the
> `orders/new` / `orders/processed` topics in `app.py`.

## Two-process

```bash
uv run litestar --app examples.mqtt.app:app run
uv run litestar --app examples.mqtt.app:app faststream run
```

## Single process

```bash
uv run litestar --app examples.mqtt.app:app run
```

## Tests

```bash
uv run pytest examples/mqtt/testing.py
```

## What this demonstrates

* `Controller` mixing HTTP siblings with a stream handler.
* Bridging an HTTP POST to an MQTT topic by injecting `mqtt: MQTTBroker` into the route handler and calling `mqtt.publish(...)` directly (Litestar DI).
* `@publisher` wrapping the subscriber's return value (FastStream-native `@broker.publisher` semantics).
* `after_startup` lifecycle hook.
* AsyncAPI docs at `/asyncapi`.
* **DI in FastStream subscribers** via `faststream.mqtt.annotations` —
  `MQTTMessage` for the raw incoming message. (FastStream does not yet
  ship a raw-client annotation for MQTT; reach for `broker._connection`
  if you need the underlying `zmqtt.MQTTClient`.)
* **DI in Litestar routes** — `GET /orders/stats` takes `mqtt: MQTTBroker`;
  the broker is registered under `BrokerConfig.name` (defaults to `"mqtt"`).

## Live broker smoke

```bash
docker compose -f examples/mqtt/docker-compose.yml up -d
uv run litestar --app examples.mqtt.app:app run
uv run litestar --app examples.mqtt.app:app faststream run
```

```bash
curl -X POST localhost:8000/orders -H 'content-type: application/json' \
     -d '{"user_id": 1, "item": "tea"}'
```

Tear down:

```bash
docker compose -f examples/mqtt/docker-compose.yml down
```

Scripted variant:

```bash
bash examples/mqtt/smoke.sh
```
