# Changelog

## 0.5.0 — 2026-03-10

- **Rename Storage to Materializer.** The `Bedrock.DataPlane.Storage` module tree has been renamed to `Bedrock.DataPlane.Materializer` to better reflect its role — materializing committed state from the write-ahead log. The `:storage` capability is now `:materializer` in cluster config, and the corresponding config key changes accordingly:

  ```elixir
  # Before
  capabilities: [:coordination, :log, :storage],
  storage: [path: working_dir]

  # After
  capabilities: [:coordination, :log, :materializer],
  materializer: [path: working_dir]
  ```

- **Add object storage layer.** New `Bedrock.ObjectStorage` module provides a backend-agnostic interface for persisting data to S3-compatible stores or the local filesystem. Supports chunked streaming reads/writes, snapshots, conditional puts, and versioned updates. See the [S3 Object Storage guide](guides/object-storage-s3.md).

- **Add durability profiles.** New `Bedrock.Durability` module lets you inspect and enforce a cluster's durability posture. Profiles evaluate whether object storage, replication, and WAL configuration meet requirements. Defaults to `:strict` mode; use `:relaxed` for local development. See the [Durability Foundation guide](guides/durability-foundation.md).

- **Add demux layer for shard-aware persistence.** New `Bedrock.DataPlane.Demux` splits committed mutations by shard and persists them asynchronously through per-shard servers and a persistence queue, without blocking commit acknowledgments. See the [Async Persistence Queue guide](guides/async-persistence-queue.md).

- **Add shard router.** New `Bedrock.DataPlane.ShardRouter` routes keys to shards via ETS ceiling search on shard boundary keys, replacing the former storage team descriptor approach.

- **Distribute system metadata through the resolver.** Commit proxies now receive differential metadata updates (keys prefixed with `\xff`) during conflict resolution, enabling proxies to stay current with cluster topology changes without a separate distribution channel.

- **Add single-resolver fast path.** Commit proxy finalization skips `Task.async` and `async_stream` overhead when only one resolver is configured, reducing latency for non-sharded workloads.

- **Add cluster bootstrap discovery.** New `Bedrock.ClusterBootstrap.Discovery` module initializes cluster state from object storage, enabling clusters to bootstrap from a durable snapshot.

- **Redesign recovery phases.** `TransactionSystemLayoutPhase` is renamed to `TopologyPhase`. `StorageRecruitmentPhase`, `VacancyCreationPhase`, and `VersionDeterminationPhase` are removed — their responsibilities are now handled by the new `MaterializerBootstrapPhase`.
