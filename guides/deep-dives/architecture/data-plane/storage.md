# Storage

[Storage](../../../glossary.md#storage) servers solve a fundamental problem in distributed databases: how to serve fast, consistent reads while maintaining strong transactional guarantees. They sit between the authoritative [Transaction](../../../glossary.md#transaction) [Log](../../../glossary.md#log) and the applications that need to read data, creating a layer that optimizes for read performance without sacrificing consistency.

**Location**: [`lib/bedrock/data_plane/storage.ex`](../../../../lib/bedrock/data_plane/storage.ex)

## The Performance Problem

When Bedrock [commits](../../../glossary.md#commit) a transaction, that transaction is immediately durable in the log servers. But serving reads directly from logs would be prohibitively slow—logs are optimized for append-only writes, not random key lookups. The solution is storage servers that maintain read-optimized copies of the data, updated asynchronously from the logs.

This creates a classic consistency challenge: how do you serve fast reads from local caches while ensuring that transactions see a consistent view of the world? Storage servers solve this fundamental tension between performance and correctness through [Multi-Version Concurrency Control (MVCC)](../../../glossary.md#multi-version-concurrency-control), [eventually consistent](../../../glossary.md#eventually-consistent) handling, and pluggable architecture that can adapt to different workloads while maintaining strict [ACID](../../../glossary.md#acid) guarantees.

## Multi-Version Time Travel

Storage servers solve the consistency problem through multi-version concurrency control. Every piece of data in Bedrock exists at multiple points in time. When a key gets updated by different transactions, storage servers keep all the historical [versions](../../../glossary.md#version) rather than overwriting the old value. This enables "time travel"—a transaction can ask for the value of a key as it existed at any point after the [minimum read version](../../../glossary.md#minimum-read-version).

This multi-version approach is what makes [Optimistic Concurrency Control (OCC)](../../../glossary.md#optimistic-concurrency-control) possible. When the system needs to detect [conflict](../../../glossary.md#conflict) between transactions, it can look at exactly which versions each transaction read and determine whether they interfered with each other. Without version history, this conflict detection would be impossible.

Version management also solves garbage collection elegantly. Storage servers can safely delete old versions once they know that no future transaction will need them, based on tracking the [minimum read version](../../../glossary.md#minimum-read-version) still in use across the cluster.

## The Eventual Consistency Dance

Storage servers maintain an eventually consistent relationship with the transaction log. Committed transactions trickle in asynchronously as storage servers pull them from log servers and apply the updates. This means there's always a window where a transaction has been committed but not yet reflected in all storage servers.

Bedrock handles this carefully through version leasing. The [Gateway](../../../glossary.md#gateway) ensures that transactions only read at versions that are guaranteed to be available on all storage servers they'll access. If a transaction tries to read at version 100, the system first confirms that all relevant storage servers have applied transactions up to at least version 100.

This coordination enables the best of both worlds: writes achieve immediate durability through the log, while reads get fast local access through storage servers. The version-based consistency model ensures that despite the asynchronous updates, every transaction sees a coherent snapshot of the data.

## Horizontal Scaling Through Partitioning

As data grows, storage servers scale horizontally through key range partitioning. Each storage server owns specific ranges of keys and only maintains data for those ranges. From a performance perspective, each storage server can optimize its storage layout and caching strategies for its specific key ranges. Hot keys can be identified and cached more aggressively, and the storage engine can be tuned for the access patterns of its particular data.

Operationally, range partitioning enables dynamic load balancing. If one key range becomes a hotspot, it can be split and redistributed across multiple storage servers. The [Director](../../../glossary.md#director) manages these range assignments and can adapt them during recovery or rebalancing operations.

## Pluggable Storage Engines

Storage servers implement an abstract interface that separates the storage logic from the engine implementation. The interface is minimal—essentially versioned key-value reads, transaction application, and recovery coordination. But this simplicity enables radical implementation differences. Some storage engines might prioritize ultra-low latency using pure in-memory storage, while others might optimize for cost using cloud object storage.

This pluggability enables experimentation and gradual migration. A cluster could run proven disk-based storage engines alongside experimental new technologies, gradually shifting load as confidence in the new engines grows.

## Recovery: Storage as Cache, Not Source of Truth

The relationship between storage servers and logs becomes crucial during recovery. Storage servers can be completely rebuilt from the transaction log, which means they're not a point of failure for data durability—that responsibility belongs entirely to the logs.

During recovery, storage servers report their current state to the Director: what's the latest version they've applied, and what's the oldest version they still retain. If a storage server has been offline and missed transactions, it simply catches up by pulling the missing transactions from log servers and applying them in order. The version-based ordering ensures that this catch-up process maintains consistency.

## Integration with the Transaction System

Storage servers integrate with the transaction system at several key points. [Transaction Builder](../../../glossary.md#transaction-builder) are their primary consumers, using "horse racing" to query multiple storage replicas in parallel and take the first successful response. The storage system also supports conflict detection indirectly by maintaining the version history that Resolvers need. Version leasing creates another integration point with the Gateway, ensuring that transactions only read at versions that are guaranteed to be available across all storage servers they'll access.

For the complete transaction flow, see **[Transaction Processing Deep Dive](../../transactions.md)**.

## Related Components

- **[Basalt](../implementations/basalt.md)**: The primary multi-version storage engine implementation
- **[Log System](log.md)**: Source of committed transactions for storage updates through continuous pulling
- **[Transaction Builder](../infrastructure/transaction-builder.md)**: Primary consumer of storage read operations with horse racing performance optimization
- **[Gateway](../infrastructure/gateway.md)**: Coordinates read version leasing to ensure Storage server data availability
- **[Director](../control-plane/director.md)**: Control plane component that manages storage recovery and key range assignment
