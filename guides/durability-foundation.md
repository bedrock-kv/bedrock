# Durability Foundation

Every committed write in Bedrock ends up in three places, in order: a log's
write-ahead log on disk, an object-storage chunk, and the materializers that
serve reads. This guide walks through that journey, what keeps it safe at
every step, and what happens when a node restarts.

One rule governs the pipeline beyond the replicated WAL: **nothing enters
object-storage or materializer durability unless it is known to be
committed.** A WAL may contain the uncommitted tail needed to decide recovery.
The sequencer tracks the highest version confirmed on every required log —
the known committed version (KCV) — and commit proxies carry it on every
push. KCV accumulates independently with `max`; it is not metadata owned by
one transaction. Everything downstream that writes durable derived state
waits for it. Because of that rule, recovery never has to undo chunks or
materializer state: rolling back the WAL tail is pointer arithmetic, never
object-store surgery.

## The Life of a Write

**A transaction commits.** The commit proxy pushes it to every required log,
and the client's commit is acknowledged only after each log has appended it
to its WAL and fsynced. At this moment the write can survive a crash — but
it lives only in the WALs.

**The log hands it to its Demux.** Pushes name their predecessor, so a future
link waits until the durable WAL prefix reaches it. When the gap closes, the
log appends the connected chain and hands every original encoded binary to
Demux in chain order; Demux performs the only per-shard slicing. A newer KCV
can advance Demux independently while a transaction waits, without claiming
that the transaction `high_water` advanced. Every log replica owns its own
anonymous ShardServer for each shard it touches; the child is discoverable
only through that log's Demux map. Two log replicas carrying the same logical
shard therefore share neither a process nor a durability confirmation.
Heartbeat transactions carry no data and touch no shard; they exist to keep
the version clock moving.

**The Demux cuts, and shards flush.** Versions in Bedrock are microsecond
timestamps, so "every five seconds" is just integer division: when a pushed
version lands in a new five-second bucket, the previous bucket is done and
becomes a candidate cut. The cut fires once the known committed version has
reached it — never before — and every ShardServer then writes everything at
or below the cut to object storage as a chunk. A chunk is named for the last
commit it contains, which lets any reader find the chunk covering a given
version with one cheap listing call. Because every replica sees the same
versions and computes the same cuts, replicas produce byte-identical chunks,
and "that file already exists" counts as a successful write.

**The floor rises, and the WAL trims.** When a shard's chunk write is
confirmed, the shard reports the cut itself as durable — a promise that
everything it has ever seen at or below that version is safe. A shard with
nothing buffered confirms instantly, so idle shards never hold things up; a
Demux with no shards at all reports its last completed cut, so even a
heartbeat-only log makes progress. Each report carries the ShardServer pid,
and the Demux accepts it only when that pid is the current child for the shard.
The Demux takes the minimum across its own children and tells only its owning
log, so one replica cannot trim on another replica's confirmation. The log
recycles every WAL segment that falls entirely below that floor. The active
segment rolls to a fresh preallocated file on the same five-second boundaries
the cuts use, so there is always a finished segment for the floor to catch — a
log's disk footprint stays proportional to a few seconds of traffic, not to
its lifetime.

Each new WAL segment durably records the prior WAL tip as `previous_version`
in its header before the preceding segment can become trim-eligible. After a
cold restart, the oldest retained header therefore preserves `available_after`:
the exact exclusive replay cursor before retained data. This cursor is distinct
from the first retained transaction and remains meaningful across numeric
version gaps.

The commit acknowledgment never waits for any of this. Clients are
acknowledged on WAL fsync; chunk writes and floor advancement happen behind
the scenes, and the floor only ever moves forward.

## How Reads Stay Current

Materializers — the processes that serve reads — never touch the WAL. Each
one serves a single shard and lives on one continuous stream from its
ShardServer: chunks for history, the in-memory buffer for recent data. The
two regions always meet, because a buffered entry is only dropped once its
chunk write is confirmed. A materializer that falls behind is not an
emergency; it is simply further back on the stream, reading chunks, and the
cost falls on it alone.

Every pull reply carries currency: "here is your data" or "nothing for you,
but you are current through version v." Idle shards learn that high-water
mark by subscription, not by polling — a parked materializer asks its Demux
once, and the Demux answers the moment it knows more. There are no timers
anywhere in the read path: a read at any version a client can legally hold
resolves purely from messages already in flight.

Materializers apply what they receive immediately, so reads are fresh — but
they only write to their own disk up to the known committed version. That
is the same rule as the chunk cuts, applied one layer down, and it buys the
same prize: a materializer's disk can never contain anything a recovery
would take back.

## What Happens on Restart

When a node comes back, the coordinator elects a director and recovery
rebuilds the transaction system from what survived. Three things make a
restart cheap and safe:

- **Logs are generational.** Each recovery recruits a fresh set of logs and
  replays the surviving WALs into them. Because the old WAL was trimmed,
  the replay copies only the untrimmed tail — a few seconds of
  transactions — never the cluster's whole history. Everything older is
  already in object-storage chunks.
- **Materializers are reused.** Recovery locks every advertised
  materializer, learns each one's shard and durable position, and hands the
  survivors back their shards. A materializer resumes streaming from its
  own applied position; one restored from an old snapshot just has more
  stream to drink. Only a genuinely lost shard gets a fresh materializer,
  which rebuilds from chunks.
- **The layout is the source of truth for what exists.** When recovery
  completes and its transaction system layout is durable, every foreman
  compares the workers it hosts against the layout and retires the ones the
  layout does not reference: previous-generation logs whose data was
  replayed forward, and any strays left by interrupted recovery attempts.
  Creation happens only through recovery; destruction happens only through
  this reconciliation; nothing accretes.

Recovery attempts themselves are pure functions of the coordinator's current
service view, retried whenever the view changes. An attempt that fires
before workers have registered fails in microseconds and leaves nothing
behind; the next registration triggers the attempt that succeeds. A typical
restart converges in two attempts and under a second, regardless of how old
the cluster is.

## Runtime Guardrails

Profile checks validate:

- minimum replication parameters (`desired_replication_factor`, `desired_logs`);
- persistent coordinator configuration;
- persistent path configuration for coordinator, log, and materializer roles.

Configuration is additive. Runtime default mode is `:strict`.

Explicit relaxed override for development or single-node rollout:

```elixir
config :bedrock, MyCluster,
  durability_mode: :relaxed,
  durability: [
    desired_replication_factor: 3,
    desired_logs: 3
  ]
```

Strict mode (default) fails fast when requirements are not met:

```elixir
config :bedrock, MyCluster,
  durability_mode: :strict
```

Use `:relaxed` only when intentionally accepting profile warnings during staged
rollout.

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

Trim floor and WAL growth:

- `[:bedrock, :demux, :durability, :floor_advanced]` — carries the shard
  currently pinning the floor and the active shard count
- `[:bedrock, :log, :trim]` — raw observability on each trim: floor, WAL
  tip, lag, and retained/recycled segment counts. Shale reports these facts
  and does not judge them — alerting thresholds and admission policy belong
  to the operator (or a future ratekeeper), not to a single log.
- `[:bedrock, :log, :wal_limit_exceeded]` — an error-severity, recovery-required
  signal from the opt-in `reject_pushes_above_lag_us` safety fuse, carrying the
  floor, WAL tip, refused prospective version, lag, limit, and queued count.
  Crossing the limit invalidates the current epoch; it is not an instruction to
  retry the assigned version chain in place.

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
BEDROCK_INCLUDE_DISTRIBUTED=1 mix test --include distributed test/bedrock/distributed/minio_durability_test.exs
```

## Migration Notes

1. Local filesystem remains the default object storage backend.
2. S3 migration is additive: enable backend config without removing existing
   LocalFilesystem paths until cutover is validated.
3. Runtime default is `:strict`; under-provisioned profiles fail startup unless
   `durability_mode: :relaxed` is set explicitly.
4. Use explicit `:relaxed` only for staged rollout, then remove it once profile
   and durability gates are consistently passing.
5. Keep WAL trim and durability telemetry on dashboards during the transition.

## Known Limitations

1. MinIO is the current primary S3 validation target; AWS S3 parity hardening is
   a follow-on track.
2. Distributed durability coverage currently focuses on foundational scenarios
   (restart, transient partition/retry).
3. Recovery corruption handling is fail-fast by design; operational runbooks
   should include explicit remediation for damaged objects.
