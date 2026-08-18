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

`ShardServer` routes chunk flush work through `PersistenceWorker`:

1. Every ShardServer is an anonymous child of one log replica's Demux; the
   Demux's shard map is its only registry, so persistence confirmation is
   replica-local even though deterministic chunk objects are shared.
2. `push/4` only updates the in-memory buffer — ShardServers never decide
   when to flush. The Demux commands `{:flush, cut_version}` on deterministic
   version-time boundaries, gated on the known committed version.
3. Worker persists chunk payloads out-of-band (at most one flush in flight
   per shard; a flush that exhausts its retries crashes the ShardServer so
   the linked Demux → log chain converts it into a recovery instead of a
   silent wedge).
4. `ShardServer` confirms the cut only after receiving
   `{:flush_persisted, cut_version}` confirmation; buffered entries stay
   pullable until then.

This keeps pushes non-blocking while preserving explicit durability
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
