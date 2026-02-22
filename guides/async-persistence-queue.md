# Async Persistence Queue (Demux Foundation)

`Bedrock.DataPlane.Demux.PersistenceQueue` and
`Bedrock.DataPlane.Demux.PersistenceWorker` provide bounded queue primitives for
future non-blocking shard persistence.

This foundational slice is intentionally additive:

- It introduces queue/worker infrastructure without changing current Demux or
  `ShardServer` durability behavior.
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

Later slices route shard chunk persistence through these primitives and tie
durability watermark progression to confirmed object-store writes.
