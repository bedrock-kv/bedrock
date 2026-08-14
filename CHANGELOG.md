# Changelog

## 0.5.2 — 2026-08-14

- **Fix serializable isolation for read misses and key-selector reads.** Read conflicts are now registered at read-issue time, matching FoundationDB's `Transaction::get` semantics. Previously a point read that missed storage (`{:error, :not_found}`) never entered the transaction's read conflict set — so two concurrent transactions could both read-miss the same key, both write it, and both commit, silently violating serializability. Key-selector reads now also record the full scanned span (FDB's `extraConflictRanges` behavior): selector resolution depends on every key between the anchor and the resolved key, so an insert anywhere in that span now correctly conflicts, for both point selectors and selector-bounded range reads.

- **Fix `Repo.rollback/1` crashing instead of rolling back.** Calling `rollback/1` inside `transact` threw an uncaught throw that crashed the caller, due to a shape mismatch between the thrown tuple and the catch clause. It now rolls back the transaction and returns `{:error, reason}` as documented.

- **Fix crash in Olivine materializer waitlisted reads.** A read that waited for a not-yet-available version and was notified without an async reply function crashed the reader during the synchronous fetch path. Waitlist notification now handles synchronous results correctly, and a process-dictionary leak in waitlist timing tracking was removed.

- **Broader test coverage and CI matrix.** Added behavioral test suites across the transaction lifecycle, commit finalization, directory partitions, coordinator durability, Basalt logic, and the Olivine materializer (several of the fixes above were found this way). CI now also tests Elixir 1.19.5 and 1.20.2 on OTP 28.3.

## 0.5.1 — 2026-07-11

- **Fix materializer recovery on fresh clusters.** Fresh Bedrock 0.5 layouts use empty log descriptors because shard-to-log routing is computed at runtime, but recovery still filtered materializer unlock logs by shard tag — so an empty descriptor matched no shard and fresh-cluster materializers started with no logs to pull committed transactions from. Recovery now treats empty log descriptors as runtime-routed logs, while legacy tag-filtered descriptors remain scoped to their matching shard.

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
