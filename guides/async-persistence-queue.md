# Async Persistence Queue

Every shard's chunk writes go through a small, bounded pipeline:
`Bedrock.DataPlane.Demux.PersistenceQueue` holds the work,
`Bedrock.DataPlane.Demux.PersistenceWorker` performs it. Together they keep
the push path non-blocking — a `ShardServer` never waits on object storage —
while durability is only ever reported after a write has actually been
confirmed.

## How a chunk becomes durable

1. Every ShardServer is an anonymous child of one log replica's Demux; the
   Demux's shard map is its only registry, so persistence confirmation is
   replica-local even though deterministic chunk objects are shared.
2. `ShardServer.push/4` only updates the in-memory buffer — ShardServers
   never decide when to flush. The Demux commands `{:flush, cut_version}` on
   deterministic version-time boundaries, and a cut candidate fires only once
   the known-committed version has reached it, so a chunk can never contain a
   version a recovery would discard.
3. The ShardServer hands the flush payload to its PersistenceWorker and keeps
   going. At most one flush is in flight per shard; the flushed entries stay
   in the buffer — still pullable — until the write confirms.
4. The worker writes the chunk with `put_if_not_exists`. Because every
   replica produces byte-identical chunks, `{:error, :already_exists}` is a
   truthful confirmation, not a conflict.
5. On `{:flush_persisted, cut_version}`, the ShardServer evicts the flushed
   entries and reports the cut as its floor contribution to the Demux, which
   aggregates the per-shard floors into the log's durability watermark.

A flush that exhausts its retries crashes the ShardServer. That is
deliberate: the linked ShardServer → Demux → log chain turns a permanently
failed write into a recovery instead of a silently frozen trim floor.

## Queue semantics

- Bounded capacity across pending, in-flight, and scheduled-retry entries.
- FIFO dequeue order for ready entries.
- Explicit `ack` and `nack` handling using dequeue tokens.
- Retry scheduling with bounded attempts and exponential backoff.

## WAL trim safety boundary

`Log.Shale.Server` treats `min_durable_version` as a monotonic watermark and
only trims WAL segments that are fully behind that boundary.

- Older watermark updates are ignored (no regression).
- Segment trimming is gated on confirmed durable watermark progression.
- Segments that straddle the boundary are retained.

## Recovery and corruption handling

Chunk replay fails fast on decode/header corruption instead of silently
skipping damaged objects.

- `ChunkReader` raises `ChunkReader.ReadError` by default when chunk metadata
  or content cannot be decoded.
- `ShardServer.pull/3` surfaces this as
  `{:error, {:storage_read_failed, reason}}`.
- `ChunkReader.list_chunk_metadata/2` supports `on_error: :skip` only for
  explicit best-effort tooling paths.

## Observability signals

Queue pressure:

- `[:bedrock, :demux, :persistence_queue, :enqueue]`
- `[:bedrock, :demux, :persistence_queue, :dequeue]`
- `[:bedrock, :demux, :persistence_queue, :backpressure]`
- `[:bedrock, :demux, :persistence_queue, :retry_scheduled]`
- `[:bedrock, :demux, :persistence_queue, :retry_dropped]`

Measurements include lag/backlog counts (`pending`, `scheduled`, `in_flight`,
`lag`) plus capacity context.

Persistence outcomes:

- `[:bedrock, :demux, :persistence, :write, :ok]`
- `[:bedrock, :demux, :persistence, :write, :error]`
- `[:bedrock, :demux, :persistence, :watermark, :advanced]`
- `[:bedrock, :demux, :durability, :floor_advanced]`

Suggested alerts:

- sustained non-zero queue backpressure events;
- increasing retry scheduling without matching write success;
- stalled watermark advancement for active shards.

See `durability-foundation.md` for the full life of a write — from commit
through cuts, chunks, and WAL trimming — and the Shale and Olivine guides for
the log and materializer ends of the same pipeline.
