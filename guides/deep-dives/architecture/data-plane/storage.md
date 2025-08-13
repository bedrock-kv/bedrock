# Storage

[Storage](../../../glossary.md#storage) servers solve a fundamental problem in distributed databases: how to serve fast, consistent reads while maintaining strong transactional guarantees. They sit between the authoritative [Transaction](../../../glossary.md#transaction) [Log](../../../glossary.md#log) and the applications that need to read data, creating a layer that optimizes for read performance without sacrificing consistency.

**Location**: [`lib/bedrock/data_plane/storage.ex`](../../../lib/bedrock/data_plane/storage.ex)

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

## Cross-Component Workflow Integration

### Transaction Processing Flow Role

Storage serves as the **read performance layer** in Bedrock's core processing workflow:

**Gateway → Transaction Builder → Commit Proxy → Resolver → Sequencer → Storage**

**Workflow Context**:

1. **Read Operations**: Serves versioned key-value reads to Transaction Builder processes
2. **MVCC Support**: Maintains multiple versions of data to support consistent snapshots
3. **Asynchronous Updates**: Continuously pulls committed transactions from Log servers for eventual consistency
4. **Version Coordination**: Participates in version leasing to ensure read consistency
5. **Recovery Support**: Reports durability state and rebuilds from authoritative transaction logs

**Key Handoff Points**:

- **To Transaction Builder**: Serves versioned read operations
  - **Horse Racing**: Transaction Builders query multiple Storage replicas in parallel for performance
  - **Version-Specific Reads**: Serves data at specific read versions for MVCC consistency
  - **Key Range Partitioning**: Each Storage server handles specific key ranges for horizontal scaling
  - **Performance Optimization**: Transaction Builders cache fastest Storage servers for subsequent reads
  - **Error Handling**: Read failures trigger Transaction Builder retry logic with alternative Storage servers

- **From Log System**: Continuously pulls committed transactions for local state updates
  - **Asynchronous Pulling**: Storage servers pull from Log servers to maintain eventually consistent state
  - **Version Ordering**: Transactions applied in strict version order to ensure consistency
  - **Catch-up Process**: Storage servers can pull specific transaction ranges to recover from downtime
  - **Long Polling**: Efficient subscription mechanism for receiving new transactions with minimal latency

- **Version Leasing Integration**: Coordinates with Gateway for read consistency
  - **Minimum Version Tracking**: Participates in cluster-wide minimum read version calculation
  - **Garbage Collection**: Safely removes old versions based on minimum read version guarantees
  - **Availability Confirmation**: Gateway ensures Storage servers have applied transactions up to read version before leasing

**Error Propagation**:

- **Read Failures**: Individual Storage server failures → Transaction Builder horse racing → automatic failover to replica Storage servers
- **Log Pulling Failures**: Log unavailability → Storage server catch-up process → eventual consistency maintained through other Log replicas
- **Version Unavailability**: Requested version not yet available → Transaction Builder retry or abort → version leasing prevents this scenario

**Integration with Recovery Process**:

- **Recovery Reporting**: Reports current durable version and minimum retained version to Director
- **State Validation**: Director uses Storage server states to calculate cluster-wide durable version baseline
- **Catch-up Coordination**: Offline Storage servers catch up by pulling missing transactions from Logs
- **Range Assignment**: Receives key range assignments from Director via transaction system layout

### Log Replication Workflow Role

Storage participates in the **log-to-storage replication** workflow:

**Log → Storage → Application Reads**

**Workflow Context**:

1. **Continuous Synchronization**: Maintains eventually consistent copies of committed transaction data
2. **Multi-Version Persistence**: Stores historical versions to support MVCC read operations
3. **Performance Optimization**: Provides local, fast access to data without Log server query overhead
4. **Fault Tolerance**: Multiple Storage replicas ensure read availability despite individual server failures

**Integration Details**:

- **Pull-Based Updates**: Storage servers initiate transaction pulling rather than receiving pushes
- **Version-Based Consistency**: Transaction version numbers ensure consistent ordering across replicas
- **Range-Based Distribution**: Each Storage server pulls only transactions affecting its assigned key ranges
- **Pluggable Architecture**: Abstract interface allows different storage engine implementations

For the complete transaction flow, see **[Transaction Processing Deep Dive](../../deep-dives/transactions.md)**.

## Related Components

- **[Basalt](../implementations/basalt.md)**: The primary multi-version storage engine implementation
- **[Log System](log.md)**: Source of committed transactions for storage updates through continuous pulling
- **[Transaction Builder](../infrastructure/transaction-builder.md)**: Primary consumer of storage read operations with horse racing performance optimization
- **[Gateway](../infrastructure/gateway.md)**: Coordinates read version leasing to ensure Storage server data availability
- **[Director](../control-plane/director.md)**: Control plane component that manages storage recovery and key range assignment
