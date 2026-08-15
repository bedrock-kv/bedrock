# Async Persistence Queue (Demux Foundation)

`Bedrock.DataPlane.Demux.PersistenceQueue` and
`Bedrock.DataPlane.Demux.PersistenceWorker` provide bounded queue primitives for
future non-blocking shard persistence.

This foundational slice is intentionally additive:

- It introduces queue/worker infrastructure without changing current Demux or
  external APIs.
- It emits queue lag and backpressure telemetry for capacity planning.
- It includes deterministic unit tests for queue ordering and retry scheduling.

## Queue Semantics

- Bounded capacity across pending, in-flight, and scheduled-retry entries.
- FIFO dequeue order for ready entries.
- Explicit `ack` and `nack` handling using dequeue tokens.
- Retry scheduling with bounded attempts and exponential backoff.

## Telemetry Events

The queue emits these additive events:

- `[:bedrock, :demux, :persistence_queue, :enqueue]`
- `[:bedrock, :demux, :persistence_queue, :dequeue]`
- `[:bedrock, :demux, :persistence_queue, :backpressure]`
- `[:bedrock, :demux, :persistence_queue, :retry_scheduled]`
- `[:bedrock, :demux, :persistence_queue, :retry_dropped]`

Measurements include lag/backlog counts (`pending`, `scheduled`, `in_flight`,
`lag`) plus capacity context.

## Follow-On Integration

`ShardServer` routes chunk flush work through `PersistenceWorker`, but never
decides when to flush:

1. `push/4` updates the in-memory buffer (and the shard's version currency —
   every push carries the known-committed version).
2. The Demux commands deterministic cuts on fixed version-time boundaries via
   `{:flush, cut_version}`, releasing each cut only once the known-committed
   version has reached it — nothing not known-committed ever becomes durable.
3. The `ShardServer` enqueues everything at or below the cut as one chunk,
   named for the last commit it contains; the worker persists it out-of-band.
   An empty buffer confirms the cut immediately, so idle shards never pin the
   trim floor.
4. The `ShardServer` advances its durable watermark only after receiving
   `{:flush_persisted, cut_version}` confirmation, reporting the cut itself as
   its floor contribution.

Because every replica sees the same slices and the same cuts, chunks are
byte-identical across replicas and replays — `put_if_not_exists` answering
`{:error, :already_exists}` counts as a successful confirmation.

A flush that exhausts its retries can never confirm its cut; wedging silently
would freeze the trim floor. The worker's `on_drop` hook instead crashes the
`ShardServer` with `{:flush_permanently_failed, …}`, and the link chain
(ShardServer → Demux → Log) turns that into a recovery.

## WAL Trim Safety Boundary

`Log.Shale.Server` treats `min_durable_version` as a monotonic watermark and
only trims WAL segments that are fully behind that boundary.

- Watermarks are honored only while the log is `:running`, and only from the
  current Demux incarnation (they are pid-tagged; a stale incarnation after a
  recovery reset cannot advance the floor).
- The floor is clamped to the WAL's last version; older watermark updates are
  ignored (no regression).
- Segment trimming is gated on confirmed durable watermark progression.
- Segments that straddle the boundary are retained, as is the active segment.

## Recovery and Corruption Handling

Chunk replay now fails fast on decode/header corruption instead of silently
skipping damaged objects.

- `ChunkReader` raises `ChunkReader.ReadError` by default when chunk metadata or
  content cannot be decoded.
- `ShardServer.pull/3` surfaces this as
  `{:error, {:storage_read_failed, reason}}`.
- `ChunkReader.list_chunk_metadata/2` supports `on_error: :skip` only for
  explicit best-effort tooling paths.

## Observability Signals

Queue and persistence telemetry now provide direct instrumentation points:

- Queue pressure:
  - `[:bedrock, :demux, :persistence_queue, :enqueue]`
  - `[:bedrock, :demux, :persistence_queue, :dequeue]`
  - `[:bedrock, :demux, :persistence_queue, :backpressure]`
  - `[:bedrock, :demux, :persistence_queue, :retry_scheduled]`
  - `[:bedrock, :demux, :persistence_queue, :retry_dropped]`
- Persistence outcomes:
  - `[:bedrock, :demux, :persistence, :write, :ok]`
  - `[:bedrock, :demux, :persistence, :write, :error]`
  - `[:bedrock, :demux, :persistence, :watermark, :advanced]`

Suggested alerts:

- sustained non-zero queue backpressure events;
- increasing retry scheduling without matching write success;
- stalled watermark advancement for active shards.
