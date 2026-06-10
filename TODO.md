# TODO

- Litestar HTTP-scoped deps inside subscribers: Requires per-message DI scope simulation
* **HTTP-as-publisher** — publishes the validated request body, alongside with cookies, queries, etc. to the broker and returns a fixed response immidiately. Decorated method becomes a subscriber for that message. Useful for fire-and-forget enqueue endpoints.

## Inspiration from litestar-saq

- Admin/observability controller (`web_path`/`web_guards`/`web_include_in_schema`) — opt-in `/faststream` UI with broker/subscriber/message stats.
- `use_server_lifespan` flag toggling between Litestar startup hooks vs. server lifespan context manager.
- `_SpawnLoggingConfig`-style spawn-safe config/logging preparation for multi-process broker workers (neutralize unpicklable `QueueHandler`s before `multiprocessing.Process`).

---

## Spec: Worker Process with Server Mode

> An embedded HTTP server launched alongside the broker(s) by `faststream run`, exposing operator-facing endpoints. Aimed at k8s deployments where the FastStream worker runs in its own pod (no co-located HTTP app to attach probes to).
>
> A v1+v2 implementation was prototyped and reverted; the design decisions below are kept so the next pass can pick up from a settled contract.

### What we expect

- **HTTP layer**: embedded Litestar app (decision locked — same framework as the parent, zero extra deps, OpenAPI/guards available later if endpoints grow).
- **Activation**: opt-in via CLI flags on `faststream run` (`--serve`, `--serve-host`, `--serve-port`). CLI flags override any config-attached defaults.
- **Config object**: `WorkerServerConfig` dataclass (carried on `FastStreamConfig.server`) — host/port/path_prefix/per-endpoint enable flags/log level. `HealthPathsConfig` sub-dataclass for custom probe paths.
- **Lifecycle**: server runs as an `asyncio.create_task` inside `_run_brokers_until_signal`; cancelled in the `finally` *before* broker shutdown so the load-balancer drops the pod from rotation first.
- **k8s-shaped health probe trio** (default paths: `/livez`, `/readyz`, `/startupz`):
  - **Liveness** — process-only, never broker-dependent. 200 unless we detect a genuine deadlock (no deadlock detector ships in v1; reserved). Critical: broker downtime must NOT trigger pod restart loops.
  - **Readiness** — 200 iff every broker responds to `ping()`; 503 + per-broker breakdown otherwise. Flips both ways.
  - **Startup** — sticky 200 once every broker has connected at least once. Requires a `BrokerConfig.started_once` flag set by an after-startup lifespan hook.
- **AsyncAPI surface**: HTML viewer + `.json` + `.yaml` for the combined doc, plus `/asyncapi/{broker_name}` for per-broker filtered views (404 on unknown name).
- **`/metrics`**: Prometheus exposition via `prometheus_client`. Endpoint stays mounted but returns 501 with an install hint when the package is absent (better UX than 404).
- **`/otel`**: OTel runtime *introspection* (provider class names, resource attributes, installed flag). Debug surface for "why aren't my traces showing up" — NOT a Prometheus alternative and NOT an OTLP receiver.
- **All paths prefixable** via `WorkerServerConfig.path_prefix` (for ingresses that strip `/internal` or similar).

### What we don't (out of scope for v1)

- **NOT a Prometheus-only `/metrics` surface from the OTel `MeterProvider`.** v1 just uses the global `prometheus_client` default registry. Wiring OTel metrics → Prometheus exposition belongs to a later pass once we ship OTel meters.
- **NOT an OTLP receiver.** "OTel endpoint" was interpreted as introspection (read-only), not push.
- **NOT auth on operator endpoints.** No `web_guards` parity with SAQ yet — operator endpoints are assumed pod-local or behind an ingress that handles auth.
- **NOT an HTML admin UI.** That's the separate "Admin controller" TODO item (interactive inspection, broker/subscriber/message stats). Server Mode is JSON/text endpoints only.
- **NOT a deadlock detector for liveness.** v1's liveness is hard-coded 200. Add a heartbeat or watchdog later if we see real deadlock cases in production.
- **NOT multi-process mounting.** The server runs in the same process as the brokers. A separate "spawn-safe logging" item exists for multi-process worker pools.

### Resolution-order rules (locked)

| Source | When applied | Overrides |
|---|---|---|
| `FastStreamConfig.server` (a `WorkerServerConfig`) | At plugin construction | Defaults |
| `--serve` / `--serve-host` / `--serve-port` CLI flags | At `faststream run` invocation | The config's host/port/enabled fields |

Click can't distinguish "user did not pass `--serve-port`" from "user passed the default value," so when `--serve` is set the CLI always overlays the host/port flag values onto the config. Users wanting config-only defaults should omit the CLI flags entirely.

### Open questions for v2+

- Should the server *also* be mountable on the parent Litestar app (when one exists) rather than always standalone? SAQ does this via a Controller. Could ship later as `WorkerServerConfig.mount_on_parent: bool`.
- Should health probes have a configurable timeout (currently hard-coded 2s on `broker.ping(timeout=2.0)`)? Probably yes — k8s probe timeouts can be tight.
- Per-broker readiness gating: should one unhealthy broker make the whole pod NotReady, or should the user be able to mark some brokers as "non-critical"?
