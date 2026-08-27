# The Log System

The [Log](../../../glossary.md#log) system is the [durability](../../../glossary.md#durability-guarantee) backbone of Bedrock, serving as the authoritative record of every [committed](../../../glossary.md#commit) [transaction](../../../glossary.md#transaction) in the cluster. It bridges the gap between the fast, in-memory transaction processing pipeline and the permanent storage that enables [recovery](../../../glossary.md#recovery) after failures. This makes Bedrock's strong consistency and durability guarantees possible. By providing a reliable, ordered, and durable record of all committed transactions, it enables the rest of the system to focus on performance and scalability while maintaining strict [ACID](../../../glossary.md#acid) properties. The careful design of the log interface, replication model, and recovery protocols creates a robust foundation that can handle the inevitable failures that occur in distributed systems.

**Location**: [`lib/bedrock/data_plane/log.ex`](../../../lib/bedrock/data_plane/log.ex)

## The Write-Ahead Log Pattern

In any distributed database, there's a fundamental tension between performance and durability. Bedrock resolves this by using a write-ahead log pattern where transactions are first committed to a fast, append-only log before being applied to the main materializers. This approach provides immediate durability guarantees while allowing storage updates to happen asynchronously.

The Log system acts as the single source of truth for what transactions have been committed. When the Commit Proxy decides that a batch of transactions should be committed, those transactions don't become durable until they're written to the logs. Only after all log servers acknowledge the write does the Commit Proxy inform clients that their transactions have succeeded. This design creates a clean separation of concerns—the transaction processing pipeline focuses on conflict detection and coordination, while the Log system handles the critical task of ensuring that committed data survives system failures.

## Core Operations

The Log system exposes a minimal interface with just a handful of essential operations. The `push` operation accepts a transaction, its predecessor version, and the commit proxy's known committed version (KCV). A log appends immediately when the predecessor matches its durable tip, parks a future predecessor link until the gap closes, and rejects a predecessor older than its tip. The KCV is a separate monotonic watermark, not transaction metadata: a newer valid KCV can advance downstream commitment gating even while its transaction is parked. The `pull` operation serves transaction ranges during recovery — that is its only remaining consumer. Materializers do not pull from the log: they stream their shard's slice of the transaction stream from the log's Demux (`get_shard_server/2` is the one discovery call a materializer makes against a log). Recovery operations allow logs to be locked during cluster recovery and rebuilt from other log servers when necessary.

This simple contract enables the complex behaviors that follow, while keeping the interface clean enough that different storage engines can implement it in radically different ways.

## Version-Based Ordering

Every transaction carries a version number that determines its position in the global transaction order, and every push names the preceding committed version. Versions need not be numerically consecutive: the `{predecessor, commit}` links define the chain. Logs append and acknowledge only the connected prefix, parking future links until their predecessor arrives. When a gap closes, the log drains the newly connected chain in order and forwards every original encoded transaction binary to its Demux unchanged. This ordering is crucial because it enables materializers to receive transactions in the exact same order across all replicas, ensuring that every materializer converges to the same state regardless of timing variations or processing delays.

The version-based ordering also enables efficient conflict detection throughout the system. Since every transaction has a precise position in the global sequence, components like the Resolver can determine conflicts by comparing version numbers and accessed keys.

## Replication for Absolute Durability

Bedrock runs multiple log servers, and every committed transaction is replicated to all of them. This isn't eventually consistent replication—the Commit Proxy waits for acknowledgment from every log server before considering a transaction committed. This all-or-nothing approach trades some latency for absolute durability guarantees.

The benefit of this model is that losing any single log server doesn't result in data loss. During recovery, the system can reconstruct the complete transaction history from any surviving log server. If one log server becomes temporarily unavailable, the entire commit process waits rather than proceeding with incomplete replication, ensuring that durability guarantees are never compromised.

## The Demux: Per-Shard Distribution and WAL Trimming

Each running log owns a Demux — a process tree that splits every appended transaction into per-shard slices and hands them to ShardServers. Shale does not pre-slice or rebuild transaction binaries; Demux is the sole slicing boundary. Each ShardServer is an anonymous child owned by exactly that Demux and discoverable only through its shard map. Replicated logs therefore have distinct processes and independent durability floors for the same logical shard. A ShardServer buffers its shard's recent slices in memory and, on deterministic version-time boundaries ("cuts") commanded by the Demux, persists them to object storage as chunk files. A cut only fires once the monotonically accumulated KCV has reached it, so chunks can never contain versions a recovery would discard. A KCV-only advance may release an existing pending cut, but it never advances transaction `high_water`; the next transaction delivery carries the held maximum into shard currency. Chunks are named for the last commit they contain, which lets a reader find the chunk covering any version with a single object-store listing call.

Materializers are the Demux's consumers. Each materializer discovers the replica-local ShardServer through its selected log, then streams its shard — chunks for history, the buffer for recent data, seamlessly from any starting position — and receives version currency (`high_water` and the known committed version) on every reply, so idle shards stay current without any polling. Durability reports are tagged with the child pid and accepted only by its owning Demux. The log's WAL is trimmed behind that replica's minimum durable version confirmed by object storage: readers never hold the WAL back, and a materializer that falls behind simply reads further back on the chunk stream at its own expense.

This architecture allows the system to optimize reads and writes independently. Writes go through the fast log append path for immediate durability, while reads are served from local materializers that follow their shard stream asynchronously. The version-based consistency model ensures that readers see a coherent view of the data despite this temporal separation.

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
- **[Materializer](materializer.md)**: Streams per-shard transactions from the log's Demux for local state updates
- **[Director](../control-plane/director.md)**: Control plane component that manages Log recovery and infrastructure planning
- **[Foreman](../infrastructure/foreman.md)**: Infrastructure component that creates and manages Log worker processes
