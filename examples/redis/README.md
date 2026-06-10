# Litestar + FastStream — Redis

Run Redis locally:

```bash
docker compose -f examples/redis/docker-compose.yml up -d
```

## Two-process

```bash
uv run litestar --app examples.redis.app:app run
uv run litestar --app examples.redis.app:app faststream run
```

## Single process

```bash
uv run litestar --app examples.redis.app:app run
```

## Tests

```bash
uv run pytest examples/redis/testing.py
```

## What this demonstrates

* `Controller` mixing HTTP siblings with a stream handler.
* Bridging an HTTP POST to a Redis channel by injecting `redis: RedisBroker` into the route handler and calling `redis.publish(...)` directly (Litestar DI).
* `@publisher` wrapping the subscriber's return value (FastStream-native `@broker.publisher` semantics).
* `after_startup` lifecycle hook.
* AsyncAPI docs at `/asyncapi`.
* **DI in FastStream subscribers** via `faststream.redis.annotations` —
  `RedisChannelMessage` for the raw incoming message, `Redis` for the raw
  `redis.asyncio.Redis` client, and `Pipeline` for atomic multi-op commits.
  Note: against a live Redis you can call any client method on `Redis`
  directly (e.g. `await redis.set(...)`); the example only logs because
  `TestRedisBroker` substitutes the raw client with a non-async MagicMock.
* **DI in Litestar routes** — `GET /orders/stats` takes `redis: RedisBroker`;
  the broker is registered under `BrokerConfig.name` (defaults to `"redis"`)
  and its class is exposed via `signature_namespace`, so the type hint resolves
  without explicit imports.

## Live broker smoke

```bash
docker compose -f examples/redis/docker-compose.yml up -d
uv run litestar --app examples.redis.app:app run
uv run litestar --app examples.redis.app:app faststream run
```

```bash
curl -X POST localhost:8000/orders -H 'content-type: application/json' \
     -d '{"user_id": 1, "item": "tea"}'
```

Tear down:

```bash
docker compose -f examples/redis/docker-compose.yml down
```

Scripted variant:

```bash
bash examples/redis/smoke.sh
```
