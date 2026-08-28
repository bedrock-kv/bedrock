# Olivine

[Olivine](../../../glossary.md#olivine) is Bedrock's materializer engine — the [storage](../../../glossary.md#storage) implementation that maintains versioned key-value state for a single [shard](../../../glossary.md#shard) and serves [MVCC](../../../glossary.md#multi-version-concurrency-control) reads. It builds its state from one continuous stream: a snapshot for the deep past, object-storage [chunks](../../../glossary.md#chunk) for history, and its ShardServer's in-memory buffer for the most recent transactions.

**Location**: `lib/bedrock/data_plane/materializer/olivine/`

## One Stream, From Snapshot to Live

An olivine materializer's whole life is a single loop. At startup it loads the latest snapshot for its shard from object storage (or starts empty). From there it asks its shard's [Demux](../../../glossary.md#demux) ShardServer for everything after its own applied position and stays on that stream forever. The ShardServer serves chunks for the historical range and its buffer for the recent range, and the two regions always meet — buffered entries are only evicted once their chunk write is confirmed — so there is no catch-up mode versus live mode, only *where you are on the stream*.

The materializer's only contact with a [log](../../../glossary.md#log) is discovery: `Log.get_shard_server/2`, once per session and again on failover. Which log to ask is deterministic arithmetic shared with the commit proxies (sorted log ids walked by `ShardRouter.get_log_indices/3`), and any replica works because every replica of a shard sees the same slices and the same cuts.

## Version Currency

Read versions in Bedrock are the [known committed version](../../../glossary.md#known-committed-version), so a materializer must keep its applied version current even when its shard receives no data. Every pull reply from the ShardServer carries `%{high_water, kcv}`; an empty reply means "nothing for you, but you are current through v." The puller rematerializes that as a heartbeat — an empty transaction at the high-water — fed through the normal apply path, so version advancement, read wake-ups, and window math all ride the same code as real data. The chain is entirely event-driven: no component waits on a timer, only on messages already in flight.

## Index and Window

Olivine maintains an in-memory, versioned page index over its key range. Applying a transaction produces a new version entry (a pointer-linked snapshot of the index at that version); reads at any retained version walk the entry for that version. A read above the current version waits on a waitlist and is released the moment the stream delivers that version.

Older versions leave memory through *window advancement*: versions older than the window lag (5 seconds of version-time) are evicted to the on-disk database. Eviction is clamped to the known committed version — **nothing above the KCV ever touches disk**. Since the KCV lags real time by roughly one commit batch while the window lags by seconds, the clamp is invisible in normal operation; it exists for the moments that matter.

## Recovery Is a Pointer Discard

When a cluster recovery rolls back to version RV, olivine's rollback is `IndexManager.rollback_to(RV)`: discard the in-memory version entries above RV and their pending evictions, and reset the current-version pointer. The disk is never wrong and never touched, because materializer disk ≤ KCV ≤ RV always holds — a recovery version can never undercut the known committed version at the moment of the crash. After rollback the materializer resumes its stream from its own applied position; a materializer restored from an old snapshot needs no special case, it simply has more stream to drink.

## Ingest Backpressure

The stream puller hands each batch to the server through a synchronous ingest call. When the intake queue crosses its high-water mark the server withholds the reply until the queue drains, so the puller can never outrun the applier — backpressure by holding the acknowledgment, with no additional machinery.

## Compaction and Snapshots

Background compaction rewrites the data and index files without blocking reads or ingestion, cutting over atomically when the compacted files are ready. After a successful compaction, olivine optionally uploads the result to object storage as the shard's snapshot bundle — the bootstrap point for cold-started replacements.

## Key Components

- **Server**: GenServer handling reads, ingest, lock/unlock, and compaction cutover
- **Logic**: startup, snapshot loading, streaming lifecycle, window advancement
- **Streaming**: the stream puller — discovery, replica failover, heartbeat synthesis
- **IndexManager / Index**: versioned page index, window advancement, rollback
- **Database (data + index files)**: durable storage below the window
- **Reading**: MVCC point/range reads with waitlisting for not-yet-arrived versions

## Related Components

- **[Storage System](../data-plane/storage.md)**: General materializer concepts and interface
- **[Log System](../data-plane/log.md)**: Hosts the Demux whose ShardServers feed olivine
- **[Transaction Builder](../infrastructure/transaction-builder.md)**: Primary consumer of olivine read operations
- **[Director](../control-plane/director.md)**: Control plane component that manages materializer recovery and shard assignment

## Code References

- **Main Module**: [`lib/bedrock/data_plane/materializer/olivine.ex`](../../../../lib/bedrock/data_plane/materializer/olivine.ex)
- **Server Logic**: [`lib/bedrock/data_plane/materializer/olivine/server.ex`](../../../../lib/bedrock/data_plane/materializer/olivine/server.ex)
- **Stream Puller**: [`lib/bedrock/data_plane/materializer/olivine/streaming.ex`](../../../../lib/bedrock/data_plane/materializer/olivine/streaming.ex)
- **Index Manager**: [`lib/bedrock/data_plane/materializer/olivine/index_manager.ex`](../../../../lib/bedrock/data_plane/materializer/olivine/index_manager.ex)
- **Reading**: [`lib/bedrock/data_plane/materializer/olivine/reading.ex`](../../../../lib/bedrock/data_plane/materializer/olivine/reading.ex)
