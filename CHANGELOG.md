# Changelog

## Unreleased

- **Olivine keeps append-only workloads readable after page splits.** Keys
  above every stored page boundary now extend the actual rightmost page
  instead of returning to page 0 and overlapping later ranges. Recovery
  validates complete snapshots and the recovered page chain, rejecting
  damaged indexes explicitly instead of silently routing reads through an
  ambiguous page map.

## 0.7.0 — 2026-08-28

This release moves cluster metadata out of the broadcast layout and into the
committed keyspace, and introduces the Distributor — the component that owns
shard coverage between recoveries. Together they follow FoundationDB's divide:
the keyspace carries what must be durable and ordered with the data it
describes; the broadcast carries only this epoch's wiring.

- **The shard map leaves the broadcast and rides the keyspace.** Shard
  boundaries and materializer membership were previously fields on the
  `TransactionSystemLayout` struct, republished to every node on each
  recovery — so topology could only change at a recovery, and the broadcast
  grew with the cluster. Both now live under `\xFF/system` as ordinary
  committed key-values (`shard_keys/<end_key>` and
  `materializers/<tag>/<worker_id>`), and clients resolve routing **per key**
  from a commit proxy — Bedrock's `GetKeyServerLocations` — rather than
  receiving the whole map. Answers are cached node-locally in ETS and read
  without a message to the Link, and locations are treated as hints:
  staleness costs a retry, never a wrong answer. `TransactionSystemLayout` is
  now wiring only (`epoch`, `sequencer`, `proxies`, `resolvers`, `logs`); its
  `services`, `shard_layout`, `shard_materializers`, `id`, `director`, and
  `rate_keeper` fields are gone. The durable half is a new struct,
  `Bedrock.ControlPlane.Config.CoreState` (FDB's `DBCoreState`), replacing
  `Bedrock.ControlPlane.Config.Persistence`.

- **New: the Distributor.** A per-epoch singleton, recruited by the director,
  that owns shard coverage between recoveries — the job recovery used to do
  wholesale and only at epoch boundaries. Its mid-epoch mutations are fenced
  by a keyspace-enforced write lock (`distributor_lock/`, FDB's MoveKeys lock
  port), so a superseded Distributor cannot write. It recruits materializers
  on demand, heals coverage when one dies, and parks shards that go cold.
  Uncovered tags no longer fail reads outright: a placeholder publishes
  itself as the tag's materializer and sheds `{:error, :unavailable}` — which
  the client retry loop treats as retryable and routing-invalidating — so a
  coverage gap is bounded degradation rather than an unroutable key. No
  configuration is required; the Distributor is recruited onto an existing
  materializer-capable node. New telemetry:
  `[:bedrock, :distributor, :coverage_demand | :idle_spindown]` and
  `[:bedrock, :distributor, :placeholder, :published | :parked | :forwarded | :drained | :shed]`.

- **User commits are bounded below `\xFF`.** Bedrock now enforces
  FoundationDB's system-key trust model: a mutation keyed at or above
  `Bedrock.end_of_user_keyspace/0` (`<<0xFF>>`) is rejected at commit ingress
  with a permanent `{:error, {:key_out_of_range, key}}` — not retried, not
  clamped. Only system components commit in `:system` mode, which extends the
  bound to `Bedrock.end_of_keyspace/0`. **This changes one common pattern:** a
  user `clear_range` that ends at the `:end` sentinel now fails, because
  `:end` converts to the full-keyspace bound. Use the new
  `Bedrock.end_of_user_keyspace/0` as the widest legal end key:

  ```elixir
  # before — now rejected with {:key_out_of_range, <<0xFF, 0xFF>>}
  Repo.clear_range(txn, "users/", :end)

  # after
  Repo.clear_range(txn, "users/", Bedrock.end_of_user_keyspace())
  ```

  Rejected transactions are replaced with empty ones inside the pipeline, so
  a bad range can never pollute resolver conflict history. The keyspace and
  its families are documented in the new
  [System Keyspace](guides/quick-reads/system-keyspace.md) guide.

- **Contended transactions retry with full jitter.** The retry delay was a
  doubling ceiling plus 1–3ms of jitter, so transactions contending on one
  key computed nearly the same delay, woke together, and re-collided — the
  wall clock was set by whichever contender climbed furthest up the ladder.
  The delay is now a uniform draw from the *whole* interval below the
  ceiling (FoundationDB's client backoff), so contenders spread instead of
  laddering. On 100 concurrent transactions against one counter, the tail
  collapsed 2.7–9.5× (max 1058ms/3057ms → 387ms/320ms across two runs).
  Exposed as `Bedrock.Internal.Repo.retry_delay_in_ms/1`.

- **Commit batching actually batches now.** The proxy re-armed its open batch
  with a zero timeout, which fires as soon as the mailbox empties — with
  request/response clients that is immediately, so every batch closed at one
  transaction and the configured `max_latency_in_ms` was never reached. The
  proxy now tracks a moving average of batch fill and holds the window for a
  millisecond (FDB's `COMMIT_TRANSACTION_BATCH_INTERVAL_MIN`) only while
  batches are demonstrably filling, so the finalization round is amortized
  under load while an idle proxy still never delays a lone transaction.

- **More read failures retry instead of surfacing.** Routing- and
  liveness-shaped failures (`:layout_lookup_failed`, `:no_servers_to_race`,
  `:locked`, `:version_too_old`, `:unknown`) now retry the transaction rather
  than raising, matching FDB's `wrong_shard_server` model. The definitively
  routing-shaped ones also invalidate the node's routing cache, so the retry
  refetches. `:timeout` deliberately does not evict — slow is not stale, and
  evicting on it turns overload latency into node-wide cache thrash.

- **Object storage listings never report emptiness they did not verify.**
  `ObjectStorage.list/3` returns a bare stream, which has no way to signal
  failure — so both backends turned listing errors into an early halt,
  indistinguishable from a genuinely empty prefix. Two consumers read that
  silence as fact and lost data: a running materializer fabricated currency
  at the high-water mark and advanced past versions it never received, and a
  starting materializer opened an empty database for a populated shard. The
  stream now raises `Bedrock.ObjectStorage.ListError` instead, so an empty
  stream means the prefix *is* empty. **Custom backends must propagate
  listing errors rather than halting**, and consumers of `list/3` should
  expect a raise.

- **The local object storage backend publishes atomically.** Readers of the
  object store are written against S3's contract — an object is complete or
  absent, and `put_if_not_exists` claims a key only by publishing a whole
  object. The local backend opened the key `:exclusive` and wrote to it as a
  *second* step, so between the two the key existed at zero length. A crash
  there claimed the key permanently: every later attempt gets `:eexist`, and
  callers read `{:error, :already_exists}` as success, so a short object would
  report success forever and could never be rewritten. Both operations now
  write to a scratch file in the target's own directory, fsync, and publish in
  one step — `rename` for `put/4`, `link` for `put_if_not_exists/4`. Write
  errors are returned rather than raised, so a full disk no longer becomes a
  `MatchError` with a poisoned key left behind. **Custom backends built on the
  local one should adopt the same shape**; a two-step write cannot honour the
  contract its readers assume.

  `Chunk.decode/1` also validates the data section against the directory's
  extents. A chunk torn past its directory used to decode as `{:ok, chunk}`
  and fail later as an `ArgumentError` from `binary_part/3`, deep inside a
  materializer's catch-up; it is now a decode error at the boundary. A header
  claiming zero transactions is rejected too — `encode/1` refuses to emit one,
  so an empty directory is corruption however well-formed it looks.

- **Workers retire themselves.** The foreman no longer decides which workers
  to reap by diffing the durable layout; it relays the layout and janitors
  what a retiring worker leaves behind. A log checks itself against the
  epoch-constant log set; a materializer asks a commit proxy whether the
  committed `materializers/<tag>` entry still names it. Retirement is also
  in-band now: the commit proxy emits a privatized copy of a membership
  clear, addressed to the shard's own tag, so a materializer whose
  assignment is withdrawn mid-epoch learns from its own stream instead of
  waiting for the next recovery broadcast.

- **The foreman's health verdict is now trustworthy.** `wait_for_healthy/2`
  rested on a verdict that four separate defects could each falsify, and a
  node with only the `:log` capability could never reach `:ok` at all — not
  slowly, but never, because the only recompute path was a cast that Shale
  never sends. Four fixes together:

  - Spin-up settles the verdict. A successful boot used to leave the state at
    the `:starting` it was constructed with, however cleanly every worker
    started.
  - The verdict is stated as precedence over the whole worker collection
    rather than as a pairwise fold, so it no longer depends on the order
    workers arrive in. Previously a foreman hosting exactly one worker, and
    that worker failed to start, reported `:starting`.
  - Recomputing health and waking waiters became one act. Removing the last
    failing worker flipped health to `:ok` and told nobody, so a caller parked
    in `wait_for_healthy/2` (default timeout `:infinity`) slept through the
    moment its condition came true.
  - The foreman monitors the workers it hosts. Health was recorded once, at
    start, and never revisited, so `{:ok, pid}` outlived the process it named
    — layout pushes went to a dead pid (and that push is how a worker learns
    it has been displaced), the coordinator kept being told a dead process was
    available, and the verdict folded over corpses. Workers are `:transient`,
    so a dead worker is marked `:stopped` and a bounded recheck adopts the
    restarted replacement rather than dropping a live worker from the roll
    call.

- **Directories without a manifest are reported, not adopted.** The foreman
  globbed every entry under its path and called each a worker, but that path
  is shared — the cluster supervisor derives `object_storage/` from the same
  `:path`, and deployments put the coordinator's `raft/` alongside it. Both
  landed in the worker map as `{:failed_to_start,
  :manifest_does_not_exist}`, as did the remains of workers whose manifests
  are gone. Those orphans are stuck by construction: retirement runs *through*
  a live worker, so a directory that starts no process can never retire
  itself, and it is retried and re-failed on every boot while holding its disk
  invisibly. Enumeration is now keyed on the manifest's presence, and
  **absence is the only thing that excludes** — a corrupt manifest is a worker
  in trouble, and one that cannot be stat'ed for any other reason is a worker
  we cannot rule out, so neither vanishes from the foreman's view. Manifest-less
  directories are named in the log at boot and left alone: they may hold a WAL,
  and deleting data on a guess is not the foreman's call. Reclamation is the
  operator's, which is why the log names the paths.

- **The write-ahead log rolls on the Demux's configured cut interval.** A
  segment holds exactly one cut bucket — that is what lets trimming drop
  history at the cut cadence even though the active segment is trim-immune.
  The two sides agreed only by coincidence: the roll boundary read the Demux's
  *default* directly, and the log's Demux was started without
  `:cut_interval_us` at all, so the documented option was unreachable from a
  log. Both now resolve through one point, and the value rides the worker's
  manifest params, so a restarted worker does not revert to the default and
  resume rolling on boundaries its already-written chunks were not cut on. A
  drift's cost was retention, not durability — an unaligned segment stays
  un-trimmable until the last cut covering it lands, and is never trimmed
  early.

- **The segment pool ceiling is enforced.** `SegmentRecycler.check_in/2` has
  always documented that it deletes a returned segment when the pool is full;
  `max_available` was fetched, threaded into state, and never compared against
  anything. Steady state hides this — one checkout per roll and one check-in
  per trim oscillates at the cap — but a trim burst does not: a jumped
  durability watermark checks in every segment below it back-to-back with no
  intervening checkout, and the pool keeps each at full `segment_size` for the
  life of the worker. At the 64 MiB production size a ten-segment burst pinned
  640 MiB that was never released. A recycler constructed with `min_available
  >= max_available` is now a startup error rather than silent degradation,
  since with no slack every cycle allocates a replacement and recycles nothing.

- **The segment recycler reports its failures instead of masking them.** Its
  post-init failure path called `stop/2` with the arguments transposed, so it
  exited with `:shutdown` and installed the real cause as its state.
  `gen_server` suppresses error reporting for `:shutdown`, so the crash was
  silent and the cause was discarded — a log whose disk filled looked like it
  had shut down politely. `:enospc` and `:eacces` now reach the log. An
  exhausted checkout also schedules a refill: the one moment the pool most
  needed one was the one moment nothing asked for it.

- **Cold shards park and revive on demand.** A materializer with an
  `idle_timeout` set uploads a snapshot and exits when no *client reads*
  arrive within the window (pulls and transaction application keep a shard
  fresh, not hot). The Distributor parks the tag rather than re-recruiting;
  the next read's coverage demand revives it from the uploaded snapshot plus
  the log suffix. The system shard is exempt.

- **Resolver protocol simplified.** Converged verdicts replace the
  hold/confirm metadata handshake, resolvers replay retried batches instead
  of fabricating verdicts for them, each proxy receives an exact tiling
  metadata window rather than an ack protocol, and resolver calls fail fast
  instead of retrying internally. The removed telemetry events are
  `[:bedrock, :commit_proxy, :resolver, :retry | :max_retries_exceeded]` and
  `[:bedrock, :resolver, :resolve_transactions, :processing | :reply_sent | :validation_error | :waiting_list | :waiting_list_validation_error]`;
  `[:bedrock, :data_plane, :commit_proxy, :ingress_validation_failed]` is
  new. `Bedrock.DataPlane.Resolver.Validation` is removed — keyspace bounds
  are validated in the commit pipeline, per transaction.

- **Removed modules.** `Bedrock.ControlPlane.Config.Persistence` (replaced by
  `CoreState`), `Bedrock.DataPlane.Resolver.Validation`,
  `Bedrock.SystemKeys.MaterializerList`, `Bedrock.SystemKeys.OtpRef`, and
  `Bedrock.SystemKeys.ShardMetadata` (replaced by
  `Bedrock.SystemKeys.Values`, a single tuple-encoding codec, and
  `Bedrock.SystemKeys.Reader`). System-key values now use explicit versioned
  encodings; decoders handle durable bytes, never raise, and never create
  atoms.

  Four more modules go with a dead-code sweep:
  `Bedrock.ObjectStorage.ChunkWriter` (superseded — the demux shard server
  encodes and writes chunks directly through `Chunk.encode/1`, cutting on the
  demux's own interval), `Bedrock.ControlPlane.RateKeeper` (a name with no
  implementation since the initial cut), `Bedrock.DataPlane.Proxy` (a read-version
  proxy; clients get read versions through the transaction builder and commit
  proxies), and `Bedrock.ControlPlane.Director.ChangingParameters` (uncalled
  since 2025-08).

- **The guides are published, and accurate.** The README linked 18 guides that
  were never listed in `mix.exs` extras, so every architecture link on the
  hexdocs landing page resolved to nothing. All of them now ship — along with
  the per-component and per-recovery-phase guides they link to — grouped into
  Quick Reads, Deep Dives, Recovery Phases, Component Deep Dives, Durability,
  and Reference. `mix docs` reports zero broken references, down from 174.

  Publishing them meant correcting the drift they had accumulated first. Every
  `lib/bedrock/**.ex` path and `Bedrock.*` module named in a published guide is
  now verified to exist. The substantive fixes: the Gateway → Link rename, the
  storage-team → shard model, `key_codecs`/`value_codecs` (which `use
  Bedrock.Repo` never accepted) replaced with the `Bedrock.Keyspace` encodings
  that actually exist, a Transaction System Layout guide rewritten for the
  wiring-only TSL, and four recovery-phase guides retired — one of which
  described materializer recruitment as *excluding* prior-layout services, the
  exact inverse of the re-adoption the phase performs.

  The README was also rewritten around Bedrock's three pillars (FoundationDB
  lineage, object-storage durability, pure BEAM zero-copy), the new System
  Keyspace guide documents every `\xFF/system` family and who writes it, and CI
  now covers OTP 29.

- **The key and value encodings are documented API.** `Bedrock.Encoding` and
  its three implementations carried `@moduledoc false` while the guides told
  you to pass `Encoding.Tuple` as a `key_encoding:` — the package was hiding
  modules it asked you to name. They are now documented, with the property
  that actually governs the choice made explicit: an encoding used for **keys**
  must be order-preserving, so that the byte order of packed keys matches the
  logical order of the values inside them and a range read returns what you
  meant.

  | Encoding | Accepts | Order-preserving | Use for |
  |---|---|---|---|
  | `Bedrock.Encoding.Tuple` | tuples, lists, binaries, integers, floats, `nil` | yes | keys |
  | `Bedrock.Encoding.None` | binaries only | yes (identity) | keys or values |
  | `Bedrock.Encoding.BERT` | any Elixir term | **no** | values |

  `BERT` is documented as values-only for that reason, and carries a note that
  `unpack/1` uses `:erlang.binary_to_term/1` without `:safe` — fine for values
  your own application wrote, not for bytes from anywhere else.

- **flatbuffer is now `~> 0.5`.** The 0.3.1 tarball shipped the *generated*
  leex/yecc output (`src/*.erl`) alongside the `.xrl`/`.yrl` grammars it was
  built from, so the schema parser Bedrock compiled was whatever the
  package maintainer's local OTP happened to emit at publish time rather than
  a build from the grammars in source control — and a generated file newer
  than an edited grammar would ship stale, silently. 0.5.0 ships the grammars
  only, and they are regenerated during Bedrock's own build.

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
