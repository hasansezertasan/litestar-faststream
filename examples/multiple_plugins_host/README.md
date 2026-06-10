# Litestar + FastStream — Multi-broker (Kafka + Redis)

Two brokers under a single `FastStreamPlugin`. Kafka is the durable log; Redis fans out notifications. One combined AsyncAPI document is served at `/asyncapi`.

Run both brokers locally:

```bash
docker compose -f examples/multiple_plugins_host/docker-compose.yml up -d
```

## Two-process

```bash
uv run litestar --app examples.multiple_plugins_host.app:app run
uv run litestar --app examples.multiple_plugins_host.app:app faststream run
```

`faststream run` consumes from every registered broker. Use `--broker kafka` or `--broker redis` to scope the worker to one.

## Single process

```bash
uv run litestar --app examples.multiple_plugins_host.app:app run
```

## Tests

```bash
uv run pytest examples/multiple_plugins_host/testing.py
```

## What this demonstrates

* `FastStreamPlugin(FastStreamConfig(brokers=[...]))` hosting two brokers in one Litestar app.
* `@subscriber("...", plugin="kafka")` / `@publisher("...", plugin="kafka")` routing a marker to one specific broker by name.
* Combined AsyncAPI document covering both brokers at `/asyncapi`.
* Per-broker `after_startup` hooks via `plugin.after_startup("kafka")` / `plugin.after_startup("redis")`.

## Live smoke

```bash
docker compose -f examples/multiple_plugins_host/docker-compose.yml up -d
```

Then in two terminals:

```bash
uv run litestar --app examples.multiple_plugins_host.app:app run
uv run litestar --app examples.multiple_plugins_host.app:app faststream run
```

Hit the HTTP endpoint:

```bash
curl -X POST localhost:8000/orders -H 'content-type: application/json' \
     -d '{"user_id": 1, "item": "tea"}'
```

Check the combined AsyncAPI viewer at http://localhost:8000/asyncapi.

Tear down:

```bash
docker compose -f examples/multiple_plugins_host/docker-compose.yml down
```
