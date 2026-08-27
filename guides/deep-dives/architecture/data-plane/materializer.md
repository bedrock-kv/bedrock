# Materializer

[Materializer](../../../glossary.md#materializer)s solve a fundamental problem in distributed databases: how to serve fast, consistent reads while maintaining strong transactional guarantees. They sit between the authoritative [Transaction](../../../glossary.md#transaction) [Log](../../../glossary.md#log) and the applications that need to read data, creating a layer that optimizes for read performance without sacrificing consistency.

**Location**: [`lib/bedrock/data_plane/materializer.ex`](../../../../lib/bedrock/data_plane/materializer.ex)

## The Performance Problem

When Bedrock [commits](../../../glossary.md#commit) a transaction, that transaction is immediately durable in the log servers. But serving reads directly from logs would be prohibitively slow—logs are optimized for append-only writes, not random key lookups. The solution is materializers that maintain read-optimized copies of the data, updated asynchronously from a per-shard stream produced by each log's Demux.

This creates a classic consistency challenge: how do you serve fast reads from local caches while ensuring that transactions see a consistent view of the world? Materializers solve this fundamental tension between performance and correctness through [Multi-Version Concurrency Control (MVCC)](../../../glossary.md#multi-version-concurrency-control), [eventually consistent](../../../glossary.md#eventually-consistent) handling, and pluggable architecture that can adapt to different workloads while maintaining strict [ACID](../../../glossary.md#acid) guarantees.

## Multi-Version Time Travel

Materializers solve the consistency problem through multi-version concurrency control. Every piece of data in Bedrock exists at multiple points in time. When a key gets updated by different transactions, materializers keep all the historical [versions](../../../glossary.md#version) rather than overwriting the old value. This enables "time travel"—a transaction can ask for the value of a key as it existed at any point after the [minimum read version](../../../glossary.md#minimum-read-version).

This multi-version approach is what makes [Optimistic Concurrency Control (OCC)](../../../glossary.md#optimistic-concurrency-control) possible. When the system needs to detect [conflict](../../../glossary.md#conflict) between transactions, it can look at exactly which versions each transaction read and determine whether they interfered with each other. Without version history, this conflict detection would be impossible.

Version management also solves garbage collection elegantly. Materializers can safely delete old versions once they know that no future transaction will need them, based on tracking the [minimum read version](../../../glossary.md#minimum-read-version) still in use across the cluster.

## The Eventual Consistency Dance

Materializers maintain an eventually consistent relationship with the transaction log. Committed transactions arrive asynchronously over each server's shard stream: the log's Demux slices every transaction by shard, and each materializer streams exactly its own shard's slice — object-storage chunks for history, the ShardServer's in-memory buffer for recent data, one continuous stream from any starting position. Every stream reply also carries version currency ("nothing for you, but you are current through v"), so a server whose shard is idle keeps advancing without ever polling. There is still always a window where a transaction has been committed but not yet reflected in all materializers.

Bedrock handles this carefully through version leasing. The [Link](../../../glossary.md#link) ensures that transactions only read at versions that are guaranteed to be available on all materializers they'll access. If a transaction tries to read at version 100, the system first confirms that all relevant materializers have applied transactions up to at least version 100.

This coordination enables the best of both worlds: writes achieve immediate durability through the log, while reads get fast local access through materializers. The version-based consistency model ensures that despite the asynchronous updates, every transaction sees a coherent snapshot of the data.

## Horizontal Scaling Through Partitioning

As data grows, materializers scale horizontally through key range partitioning. Each materializer owns specific ranges of keys and only maintains data for those ranges. From a performance perspective, each materializer can optimize its storage layout and caching strategies for its specific key ranges. Hot keys can be identified and cached more aggressively, and the storage engine can be tuned for the access patterns of its particular data.

Operationally, range partitioning enables dynamic load balancing. If one key range becomes a hotspot, it can be split and redistributed across multiple materializers. The [Director](../../../glossary.md#director) manages these range assignments and can adapt them during recovery or rebalancing operations.

## Pluggable Storage Engines

Materializers implement an abstract interface that separates the materializer's logic from the engine implementation. The interface is minimal—essentially versioned key-value reads, transaction application, and recovery coordination. But this simplicity enables radical implementation differences. Some storage engines might prioritize ultra-low latency using pure in-memory storage, while others might optimize for cost using cloud object storage.

This pluggability enables experimentation and gradual migration. A cluster could run proven disk-based storage engines alongside experimental new technologies, gradually shifting load as confidence in the new engines grows.

## Recovery: the Materializer as Cache, Not Source of Truth

The relationship between materializers and the durable stream becomes crucial during recovery. A materializer can be completely rebuilt from its shard's snapshot and chunk history in object storage, which means it is not a point of failure for data durability—that responsibility belongs to the logs and the chunk pipeline behind them.

Materializers apply transactions eagerly for read currency but only persist to disk up to the known committed version, so their disk can never hold a version a recovery would discard. When a recovery rolls the cluster back, the rollback is a pure in-memory pointer discard—no disk surgery. A server that has been offline simply resumes its shard stream from its own applied position; the stream serves any starting point, so a stale server just has more stream to drink.

## Integration with the Transaction System

Materializers integrate with the transaction system at several key points. [Transaction Builder](../../../glossary.md#transaction-builder) are their primary consumers, using "horse racing" to query multiple materializer replicas in parallel and take the first successful response. The materializer also supports conflict detection indirectly by maintaining the version history that Resolvers need. Version leasing creates another integration point with the Link, ensuring that transactions only read at versions that are guaranteed to be available across all materializers they'll access.

For the complete transaction flow, see **[Transaction Processing Deep Dive](../../../deep-dives/transactions.md)**.

## Related Components

- **[Olivine](../implementations/olivine.md)**: The materializer engine implementation
- **[Log System](log.md)**: Hosts the Demux whose per-shard streams feed materializer updates
- **[Transaction Builder](../infrastructure/transaction-builder.md)**: Primary consumer of materializer read operations with horse racing performance optimization
- **[Link](../infrastructure/link.md)**: Coordinates read version leasing to ensure Materializer data availability
- **[Director](../control-plane/director.md)**: Control plane component that manages materializer recovery and key range assignment
