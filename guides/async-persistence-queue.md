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

`ShardServer` now routes chunk flush work through `PersistenceWorker`:

1. `push/3` updates in-memory buffer and enqueues a flush batch when thresholds
   are exceeded.
2. Worker persists chunk payloads out-of-band.
3. `ShardServer` advances durable watermark only after receiving
   `{:flush_persisted, max_version}` confirmation.

This keeps `push/3` non-blocking while preserving explicit durability
watermark progression semantics.

## WAL Trim Safety Boundary

`Log.Shale.Server` treats `min_durable_version` as a monotonic watermark and
only trims WAL segments that are fully behind that boundary.

- Older watermark updates are ignored (no regression).
- Segment trimming is gated on confirmed durable watermark progression.
- Segments that straddle the boundary are retained.

## Recovery and Corruption Handling

Chunk replay now fails fast on decode/header corruption instead of silently
skipping damaged objects.

- `ChunkReader` raises `ChunkReader.ReadError` by default when chunk metadata or
  content cannot be decoded.
- `ShardServer.pull/3` surfaces this as
  `{:error, {:storage_read_failed, reason}}`.
- `ChunkReader.list_chunk_metadata/2` supports `on_error: :skip` only for
  explicit best-effort tooling paths.
