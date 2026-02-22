# Durability Foundation (S3 + Async Persistence)

This guide consolidates the foundational durability work in Bedrock:

- durability profile contract and runtime enforcement modes;
- S3-compatible object storage backend semantics;
- non-blocking shard persistence with explicit durability watermarking;
- WAL trim safety boundaries and recovery behavior;
- telemetry and test gates for operators and maintainers.

## Foundation Scope

The current foundation covers:

1. Additive durability profile API:
   - `Bedrock.Durability.profile/1`
   - `Bedrock.Durability.require/2`
2. Additive runtime durability mode:
   - `:relaxed` (warn + continue)
   - `:strict` (fail startup on profile failures)
3. S3-compatible `Bedrock.ObjectStorage.S3` backend:
   - CRUD/list operations
   - native conditional semantics (`If-None-Match`, `If-Match`)
4. Bounded async persistence queue/worker in Demux shard flow.
5. Durable watermark progression only after confirmed object-store writes.
6. WAL trimming gated by confirmed monotonic durable watermark.
7. Fail-fast corruption handling during object-backed replay.

## End-to-End Durability Flow

1. `Demux.Server.push/3` routes shard slices without blocking on object-store I/O.
2. Each `ShardServer` enqueues flush batches to `PersistenceWorker`.
3. Worker writes chunk objects to object storage.
4. `ShardServer` advances its durable watermark only after confirmed write.
5. `Demux.Server` computes cluster `min_durable_version`.
6. Log WAL trimming advances only behind that confirmed minimum boundary.

This preserves hot-path responsiveness while keeping durability and trim behavior
explicit and monotonic.

## Runtime Guardrails

Profile checks validate:

- minimum replication parameters (`desired_replication_factor`, `desired_logs`);
- persistent coordinator configuration;
- persistent path configuration for coordinator, log, and materializer roles.

Configuration is additive:

```elixir
config :bedrock, MyCluster,
  durability_mode: :relaxed,
  durability: [
    desired_replication_factor: 3,
    desired_logs: 3
  ]
```

Strict mode fails fast when requirements are not met:

```elixir
config :bedrock, MyCluster,
  durability_mode: :strict
```

Use `:relaxed` during rollout and switch to `:strict` once infrastructure and
telemetry are stable.

## S3 Backend Configuration

Use S3 as the Bedrock object store via normalized backend config:

```elixir
config :bedrock, Bedrock.ObjectStorage,
  backend: :s3,
  s3: [
    bucket: "bedrock",
    access_key_id: "minio_key",
    secret_access_key: "minio_secret",
    scheme: "http://",
    region: "local",
    host: "127.0.0.1",
    port: 9000
  ]
```

Conditional semantics:

- `put_if_not_exists/4` returns `{:error, :already_exists}` when the key exists.
- `get_with_version/2` returns opaque version tokens from object metadata.
- `put_if_version_matches/5` returns `{:error, :version_mismatch}` on stale tokens.

These are validated in MinIO-backed `:s3` tests.

## Telemetry Signals

Durability profile:

- `[:bedrock, :durability, :profile, :ok]`
- `[:bedrock, :durability, :profile, :failed]`

Persistence queue/backpressure:

- `[:bedrock, :demux, :persistence_queue, :enqueue]`
- `[:bedrock, :demux, :persistence_queue, :dequeue]`
- `[:bedrock, :demux, :persistence_queue, :backpressure]`
- `[:bedrock, :demux, :persistence_queue, :retry_scheduled]`
- `[:bedrock, :demux, :persistence_queue, :retry_dropped]`

Persistence outcomes:

- `[:bedrock, :demux, :persistence, :write, :ok]`
- `[:bedrock, :demux, :persistence, :write, :error]`
- `[:bedrock, :demux, :persistence, :watermark, :advanced]`

## Validation Gates

Run default suite (without S3/distributed tags):

```bash
mix test
```

Run MinIO-backed S3 integration coverage:

```bash
mix test --include s3 --exclude distributed
```

Run distributed durability suite:

```bash
BEDROCK_INCLUDE_DISTRIBUTED=1 mix test --include s3 --include distributed test/bedrock/distributed/minio_durability_test.exs
```

## Migration Notes

1. Local filesystem remains the default object storage backend.
2. S3 migration is additive: enable backend config without removing existing
   LocalFilesystem paths until cutover is validated.
3. Start with `durability_mode: :relaxed` to surface profile gaps via warnings
   and telemetry.
4. Promote to `durability_mode: :strict` after profile and durability gates are
   consistently passing.
5. Keep WAL trim and durability telemetry on dashboards during the transition.

## Known Limitations

1. MinIO is the current primary S3 validation target; AWS S3 parity hardening is
   a follow-on track.
2. Distributed durability coverage currently focuses on foundational scenarios
   (restart, transient partition/retry).
3. Recovery corruption handling is fail-fast by design; operational runbooks
   should include explicit remediation for damaged objects.
