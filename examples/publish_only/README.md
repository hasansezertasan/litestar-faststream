# publish_only — split deployment example

One codebase, two processes:

* **API pod** — Litestar HTTP server. Broker is connected for publishing only; no subscriber loops.
* **Worker pod** — `litestar faststream run`. Broker fully started, subscribers consuming. No HTTP.

This is scenario B from the main [README](../../README.md#deployment-shapes-publish_only).

## Run it

Start RabbitMQ:

```sh
docker compose -f examples/publish_only/docker-compose.yml up -d
```

In one terminal, the API:

```sh
FASTSTREAM_PUBLISH_ONLY=1 uvicorn examples.publish_only.app:app
```

In another terminal, the worker:

```sh
FASTSTREAM_PUBLISH_ONLY=0 litestar --app examples.publish_only.app:app faststream run
```

> The worker process *also* loads `app.py`, but `litestar faststream run` ignores
> `publish_only` and always runs the full broker lifecycle. The env var is set
> to `0` in the worker only so this single-file example also runs cleanly if
> you point a plain `uvicorn` at it for testing.

Post an order:

```sh
curl -X POST localhost:8000/orders/ \
  -H content-type:application/json \
  -d '{"user_id": 1, "item": "book"}'
```

You'll see the publish from the API process, and the `processing order ...` log line from the worker process.

## What `publish_only=True` actually does

The plugin calls `broker.connect()` (so `broker.publish(...)` works) and skips `broker.start()` (so subscriber consume-loops never start in that process). Subscribers are still *registered* on the broker — AsyncAPI still lists them — they just don't consume.

If subscribers are registered while `publish_only=True`, the plugin logs a warning at startup naming the count, so you don't get the "why isn't my handler firing?" surprise.
