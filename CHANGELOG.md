# Changelog

## 0.6.1 — 2026-08-19

- **Security: remove hackney from the dependency tree.** An audit found the
  ex_aws default HTTP client, hackney 1.25.0, carrying four CVEs —
  CRLF/header injection via cookie options (CVE-2026-47069), a SOCKS5 TLS
  upgrade with no timeout (CVE-2026-47071), CR/LF injection in query
  parameters (CVE-2026-47075), and an SSRF allowlist bypass via
  percent-encoded hosts (CVE-2026-47076). Rather than upgrading, object
  storage now speaks through **Req** via ex_aws's own adapter, and hackney
  plus its nine transitive packages leave the lock entirely. No
  configuration is required — the S3 backend selects the client per request,
  and a custom `:http_client` in your backend config still takes precedence.
  If your application configured `:ex_aws, :hackney_opts` for Bedrock's
  benefit, use `:ex_aws, :req_opts` instead. ex_aws is now `~> 2.7`.

- **Dependency vulnerabilities now fail CI.** `mix deps.audit` (via the new
  `mix_audit` dev dependency) runs on every build, so future advisories
  surface immediately instead of accumulating.

## 0.6.0 — 2026-08-18

- **Write-ahead logs now trim themselves.** Logs previously retained their
  entire history in one untrimmable active segment, so recovery replay grew
  with cluster age (minutes of copying on old clusters). WAL segments now roll
  on the same deterministic version-time boundaries as chunk cuts, and a
  consumer-driven durability floor — advanced only after object storage
  confirms each chunk — physically recycles everything behind it. Recovery
  replay is now bounded to roughly one cut interval (~5 seconds of
  version-time) no matter how long the cluster has been running. Rollover is
  transactional: a new segment is published only after its header and first
  entry are durable, so a crash mid-roll can never lose the WAL tail.

- **Materializers stream from the Demux; Basalt is removed.** Olivine now
  pulls its shard's stream from a per-log-replica ShardServer — object-storage
  chunks for history, the in-memory buffer for recent data, one continuous
  stream — instead of drinking the whole WAL through a dedicated log-pulling
  materializer. The `Bedrock.DataPlane.Materializer.Basalt` module tree is
  gone. Version currency is fully event-driven: idle shards learn "you are
  current through v" by subscription on the next push, never by timer, so
  reads resolve in milliseconds where they previously could stall for seconds.

- **Restarts recover reliably and converge.** Restart recovery — previously
  broken end-to-end — now reuses existing materializers at their applied
  positions, seeds resolvers and shard layouts from recovered state, tolerates
  and prunes ghost directory entries left by dead nodes, remembers failed
  locks across attempts, and retains stalled-attempt progress for in-process
  retries. The durable transaction system layout is the single source of
  truth for what exists: foremen retire workers the layout no longer
  references, and late-joining nodes receive the current layout at
  registration instead of waiting for the next recovery. A restarted cluster
  converges in a couple of attempts and well under a second, regardless of
  age.

- **Serializable isolation is enforced against pruned history.** A
  transaction whose read version predates the resolver's retained conflict
  history is now aborted (`{:error, :aborted}` → retry) instead of silently
  skipping conflict detection — the read-side half of the version-floor
  design.

- **Transactions have a real deadline.** `Repo.transact/2`'s `:timeout_in_ms`
  is now enforced as one monotonic deadline across layout fetches, reads,
  commits, retries, and nested transactions, and it defaults to 5 seconds
  (pass `:infinity` to opt out). Previously, retryable failures could loop
  indefinitely and a point read could wait forever on an unresponsive
  builder. A terminal timeout surfaces the last retry reason and rolls back
  the active transaction.

- **The WAL lag limit is an epoch-fatal safety fuse.** The opt-in
  `reject_pushes_above_lag_us` log option no longer returns retryable
  backpressure. Once a version has been assigned and resolved, refusing its
  WAL append invalidates the epoch — so crossing the limit returns
  `{:error, {:recovery_required, {:wal_limit_exceeded, details}}}`, releases
  all queued successors, and emits `[:bedrock, :log, :wal_limit_exceeded]`.
  The unsound `[:bedrock, :log, :floor_lag_alarm]` event is removed;
  `[:bedrock, :log, :trim]` remains as raw per-trim observability (floor,
  tip, lag, segment counts).

- **Legacy BED0 WALs remain readable.** Cold start derives a synthetic replay
  cursor from the first retained transaction of pre-0.6 segments (new
  segments are always written in the current format), and cold-start failures
  now distinguish WAL corruption from I/O errors — transient resource
  exhaustion retries with backoff instead of being misreported as a bad
  replay cursor.

- **Compaction never loses live writes.** Olivine compaction cutover now
  restarts its stream from the compacted durable boundary, so transactions
  ingested while compaction ran are re-delivered instead of vanishing until
  the next recovery.

- **Guides reflect the new data plane.** The durability-foundation, recovery,
  data-plane, and async-persistence guides describe the shipped design —
  KCV-gated cuts, Demux streaming, reconciliation, trimming — rather than its
  history.

## 0.5.3 — 2026-08-15

- **Shrink the hex package from 8.2MB to ~300KB.** The published tarball
  inadvertently included local dialyzer PLT build artifacts, because the PLT
  cache lived in `priv/plts` and Hex packages the entire `priv/` directory by
  default. The package now declares an explicit file list (`lib`,
  `priv/schemas`, and project docs), and the dialyzer PLT cache moved out of
  `priv/`. No functional changes — 0.5.3 is identical to 0.5.2 in behavior.

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
