# The Log System

The [Log](../../../glossary.md#log) system is the [durability](../../../glossary.md#durability-guarantee) backbone of Bedrock, serving as the authoritative record of every [committed](../../../glossary.md#commit) [transaction](../../../glossary.md#transaction) in the cluster. It bridges the gap between the fast, in-memory transaction processing pipeline and the permanent storage that enables [recovery](../../../glossary.md#recovery) after failures. This makes Bedrock's strong consistency and durability guarantees possible. By providing a reliable, ordered, and durable record of all committed transactions, it enables the rest of the system to focus on performance and scalability while maintaining strict [ACID](../../../glossary.md#acid) properties. The careful design of the log interface, replication model, and recovery protocols creates a robust foundation that can handle the inevitable failures that occur in distributed systems.

**Location**: [`lib/bedrock/data_plane/log.ex`](../../../lib/bedrock/data_plane/log.ex)

## The Write-Ahead Log Pattern

In any distributed database, there's a fundamental tension between performance and durability. Bedrock resolves this by using a write-ahead log pattern where transactions are first committed to a fast, append-only log before being applied to the main storage servers. This approach provides immediate durability guarantees while allowing storage updates to happen asynchronously.

The Log system acts as the single source of truth for what transactions have been committed. When the Commit Proxy decides that a batch of transactions should be committed, those transactions don't become durable until they're written to the logs. Only after all log servers acknowledge the write does the Commit Proxy inform clients that their transactions have succeeded. This design creates a clean separation of concerns—the transaction processing pipeline focuses on conflict detection and coordination, while the Log system handles the critical task of ensuring that committed data survives system failures.

## Core Operations

The Log system exposes a minimal interface with just a handful of essential operations. The `push` operation accepts a transaction and makes it durable at a specific version number, enforcing strict ordering by rejecting out-of-sequence transactions; each push also carries the known-committed version piggybacked by the commit proxies, which downstream durability machinery gates on. The `pull` operation serves transaction ranges during recovery—recovery is its only consumer. Storage servers never pull the raw log: `get_shard_server` introduces a storage server to the ShardServer for its shard (a one-time discovery call, repeated only on failover), and all data then flows from that per-shard stream. Recovery operations allow logs to be locked during cluster recovery and rebuilt from other log servers when necessary.

This simple contract enables the complex behaviors that follow, while keeping the interface clean enough that different storage engines can implement it in radically different ways.

## Version-Based Ordering

Every transaction carries a version number that determines its position in the global transaction order. The logs enforce strict version ordering, rejecting any transaction that arrives out of sequence. This ordering is crucial because it enables storage servers to receive transactions in the exact same order across all replicas, ensuring that every storage server converges to the same state regardless of timing variations or processing delays.

The version-based ordering also enables efficient conflict detection throughout the system. Since every transaction has a precise position in the global sequence, components like the Resolver can determine conflicts by comparing version numbers and accessed keys.

## Replication for Absolute Durability

Bedrock runs multiple log servers, and every committed transaction is replicated to all of them. This isn't eventually consistent replication—the Commit Proxy waits for acknowledgment from every log server before considering a transaction committed. This all-or-nothing approach trades some latency for absolute durability guarantees.

The benefit of this model is that losing any single log server doesn't result in data loss. During recovery, the system can reconstruct the complete transaction history from any surviving log server. If one log server becomes temporarily unavailable, the entire commit process waits rather than proceeding with incomplete replication, ensuring that durability guarantees are never compromised.

## Storage Server Integration

Each log owns a Demux that slices every pushed transaction by shard and hands the pieces to per-shard ShardServers. A ShardServer buffers recent slices in memory and, on deterministic version-time boundaries ("cuts") commanded by the Demux—and released only once the known-committed version has reached them—flushes them as chunk files to object storage, named for the last commit they contain. Storage servers stream exactly their own shard from this pipeline: chunks for history, the buffer for recent data, one continuous stream that serves any starting position. Pull replies carry version currency (`%{high_water, kcv}`), so an idle shard's storage server still advances—event-driven, with no polling or timers anywhere in the chain.

This is also what lets the WAL stay bounded: once every shard confirms its chunks durable in object storage, the log trims the covered segments. The trim floor is object-storage confirmation alone—readers never hold the WAL back, and a lagging storage server is simply further back on the stream, reading chunks, with the cost falling on the laggard rather than the cluster.

This architecture allows the system to optimize reads and writes independently. Writes go through the fast log append path for immediate durability, while reads are served from local storage servers that have been updated asynchronously. The version-based consistency model ensures that readers see a coherent view of the data despite this temporal separation.

## Recovery and System Restoration

When components fail and need recovery, the Log system plays the central coordinating role. The logs serve as the definitive record of what transactions were committed and in what order, enabling precise reconstruction of system state. During cluster recovery, the Director coordinates with all available log servers to determine the complete set of committed transactions, using version ranges and durability status to make informed decisions about restoring system consistency.

Individual log servers can also recover from each other. If one log server loses its local state due to disk failure or corruption, it can rebuild its complete transaction history by pulling from another log server. This peer-to-peer recovery capability reduces the operational burden of managing log server failures.

## Abstract Interface and Pluggability

The Log system is designed as an abstract interface that can be implemented by different storage engines, enabling experimentation with diverse log technologies. This pluggable architecture allows operators to deploy different log implementations within the same cluster, each optimized for different characteristics—some might prioritize ultra-low latency using in-memory storage with battery-backed RAM, others might optimize for cost using cloud object storage, while still others could focus on maximum throughput using specialized hardware.

The minimal contract means that different log servers can coexist in the same cluster, allowing operators to match log characteristics to specific workload requirements. For instance, critical transactions could be routed to high-performance log servers while bulk operations use cost-optimized implementations. The abstraction also enables seamless experimentation with new storage technologies without disrupting the core transaction processing logic.

## See Also

- **[Transaction Processing](../../../deep-dives/transactions.md)**
- **[Recovery](../../../deep-dives/recovery.md)**

## Related Components

- **[Shale](../implementations/shale.md)**: Primary disk-based implementation of the Log interface
- **[Commit Proxy](commit-proxy.md)**: Orchestrates transaction durability through Log persistence coordination
- **[Storage](storage.md)**: Streams its shard's committed transactions from the log's Demux for local state updates
- **[Director](../control-plane/director.md)**: Control plane component that manages Log recovery and infrastructure planning
- **[Foreman](../infrastructure/foreman.md)**: Infrastructure component that creates and manages Log worker processes
