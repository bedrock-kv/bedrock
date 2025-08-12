# Shale

[Shale](../../glossary.md#shale) is Bedrock's disk-based [log](../../glossary.md#log) storage engine that provides durable [transaction](../../glossary.md#transaction) persistence. It implements the Log interface using local storage with write-ahead logging, segment files for large transactions, and efficient streaming capabilities.

**Location**: [`lib/bedrock/data_plane/log/shale.ex`](../../../lib/bedrock/data_plane/log/shale.ex)

## Design Principles

Shale is built around three core requirements: durability, ordering, and performance. Every committed transaction must survive system failures, transactions must be stored in strict version order, and the system must handle both small frequent transactions and occasional large ones efficiently.

The storage engine uses a multi-file approach because different transaction sizes have different performance characteristics. Small transactions benefit from sequential writes to a single file, while large transactions need dedicated space to avoid fragmenting the main log. This hybrid approach optimizes for the common case while handling edge cases gracefully.

## Storage Architecture

Shale organizes data into three types of files, each serving a specific purpose in the overall storage strategy.

### Write-Ahead Log (WAL) Files

The WAL is the primary storage mechanism for most transactions. It's an append-only file where transactions are written sequentially, which maximizes disk performance by avoiding random seeks. Each transaction is written with its version number, timestamp, and a checksum for integrity verification.

WAL files have a maximum size limit, typically 64MB. When a file reaches this limit, Shale creates a new WAL file and continues writing to it. This rotation prevents any single file from becoming unwieldy and enables more efficient garbage collection of old transactions.

The sequential nature of WAL writes means that even on traditional spinning disks, Shale can achieve high write throughput. Modern SSDs make this even faster, but the design doesn't depend on SSD performance.

### Segment Files

Some transactions are too large to fit efficiently in the WAL without causing performance problems for smaller transactions. These large transactions are stored in separate segment files, with the WAL containing only a reference to the segment file location.

Shale pre-allocates segment files during startup to avoid allocation delays during normal operation. When a large transaction needs storage, Shale assigns it to an available segment file and records the location in the WAL. This keeps the WAL flowing smoothly while handling large data efficiently.

Empty segment files are recycled, so the system doesn't consume unnecessary disk space over time. The allocation strategy balances having enough pre-allocated space with not wasting storage.

### Manifest and Metadata

The manifest file tracks Shale's current state, including which WAL file is active, which segment files are allocated, and what version ranges are stored. This metadata enables fast startup and recovery without scanning entire log files.

During normal operation, the manifest is updated periodically to reflect the current state. During recovery, it provides the starting point for reconstructing the full transaction index.

## Transaction Storage Process

When a transaction arrives for storage, Shale first validates that its version number follows the expected sequence. Out-of-order transactions are rejected immediately because they would break the version ordering that the rest of the system depends on.

For transactions that fit within the WAL size limit, Shale appends them directly to the current WAL file. The transaction data is written with a header containing the version, timestamp, and checksum, followed by the encoded transaction payload. This logic is implemented in [`lib/bedrock/data_plane/log/shale/writer.ex`](../../../lib/bedrock/data_plane/log/shale/writer.ex).

Large transactions follow a different path. Shale allocates a segment file, writes the transaction data there, and then writes a reference entry in the WAL. This keeps the WAL compact while ensuring that all transactions, regardless of size, are recorded in version order. Segment management is handled in [`lib/bedrock/data_plane/log/shale/segment.ex`](../../../lib/bedrock/data_plane/log/shale/segment.ex).

After writing transaction data, Shale forces a disk sync to ensure durability before acknowledging the write. This is the critical step that guarantees committed transactions survive system failures.

## Index Management

Shale maintains an in-memory index mapping version numbers to file locations. This index enables fast lookups when serving transaction pull requests without scanning log files.

The index is built incrementally as transactions are written and can be reconstructed from disk files during recovery. For performance, frequently accessed entries are kept in memory, while the complete index can be persisted periodically.

This approach balances memory usage with lookup performance. Storage servers typically pull transactions in sequential order, so the working set of index entries remains manageable even with millions of stored transactions.

## Transaction Serving

When storage servers request transactions, Shale uses the version index to locate the requested data quickly. For sequential pulls, which are the common case, Shale can optimize by reading ahead and caching likely-to-be-requested transactions.

The serving process handles both small WAL entries and large segment references transparently. Clients receive the same interface regardless of how transactions are physically stored. This streaming implementation is in [`lib/bedrock/data_plane/log/shale/pulling.ex`](../../../lib/bedrock/data_plane/log/shale/pulling.ex).

Long-running pull requests use streaming to avoid loading large result sets into memory. This enables storage servers to efficiently process transaction ranges without overwhelming Shale's memory usage.

## Recovery Implementation

Shale recovery begins by reading the manifest to understand the last known state. Then it scans WAL files to rebuild the transaction index and verify data integrity. The recovery logic is implemented in [`lib/bedrock/data_plane/log/shale/recovery.ex`](../../../lib/bedrock/data_plane/log/shale/recovery.ex).

During recovery, Shale can operate in several modes. Cold start recovery rebuilds state from disk files. Log-to-log recovery copies transactions from another Shale instance to restore a failed log server. The recovery mode is determined by what data is available and what the Director requests.

Recovery includes integrity verification, checking that transaction checksums match and that version sequences are complete. If corruption is detected, Shale can attempt to recover valid transactions while reporting problems for manual intervention.

The recovery process is designed to be conservative. If there's any doubt about data integrity, Shale will request complete recovery from another log server rather than risk serving corrupted data.

## Performance Characteristics

Shale's performance comes from several design decisions working together. Sequential writes to WAL files maximize disk throughput. Pre-allocated segment files eliminate allocation delays. In-memory indexing provides fast transaction lookups.

Write performance is primarily limited by disk sync speed, which depends on the storage hardware's durability guarantees. SSDs with power-loss protection can achieve much higher performance than traditional drives because they can complete syncs faster.

Read performance benefits from the sequential nature of most storage server requests. When servers pull transaction ranges, Shale can read large chunks efficiently and stream results without loading everything into memory.

Memory usage is controlled by limiting index cache size and using streaming for large operations. This allows Shale to handle transaction logs that are much larger than available RAM.

## Error Handling

Shale uses a fail-fast approach to error handling. When it encounters unrecoverable errors like disk corruption or hardware failures, it exits immediately rather than attempting complex recovery logic.

This design relies on the Director to detect failures and coordinate recovery. By failing quickly and cleanly, Shale avoids creating inconsistent state that would complicate recovery.

For recoverable errors like temporary disk space issues, Shale includes cleanup and compaction logic to reclaim space and retry operations. But if these attempts fail, it still chooses to exit rather than continue in a degraded state.

## Configuration and Tuning

Shale's configuration focuses on balancing durability, performance, and resource usage. WAL file sizes affect rotation frequency and recovery time. Segment pre-allocation affects memory usage and large transaction performance.

Sync policies control the trade-off between durability and performance. Immediate sync after every write maximizes durability but limits throughput. Batched sync improves performance but increases the window of potential data loss.

Buffer sizes affect memory usage and I/O efficiency. Larger buffers can improve performance for workloads with many small transactions but use more memory.

The configuration system provides reasonable defaults while allowing operators to tune behavior for specific workloads and hardware characteristics.

## Integration with Bedrock

Shale integrates with the broader Bedrock system through the Log interface. Commit proxies push transactions to Shale for durability. Storage servers pull transactions from Shale to update their local data. The main server coordination logic is in [`lib/bedrock/data_plane/log/shale/server.ex`](../../../lib/bedrock/data_plane/log/shale/server.ex).

During recovery, the Director coordinates with Shale to ensure consistent state across the cluster. Shale provides recovery information about what transactions it has stored and can serve transactions to restore other failed components.

This integration allows Shale to focus on its core responsibility—durable transaction storage—while participating in the larger system's reliability and consistency guarantees.

## Related Components

- **[Log System](../data-plane/log.md)**: General log system concepts and interface
- **[Commit Proxy](../data-plane/commit-proxy.md)**: Pushes committed transactions to Shale
- **[Storage](../data-plane/storage.md)**: Pulls transactions from Shale for local updates
- **Director**: Control plane component that coordinates Shale recovery processes

## Code References

- **Main Implementation**: [`lib/bedrock/data_plane/log/shale.ex`](../../../lib/bedrock/data_plane/log/shale.ex)
- **Server Logic**: [`lib/bedrock/data_plane/log/shale/server.ex`](../../../lib/bedrock/data_plane/log/shale/server.ex)
- **Storage Engine**: [`lib/bedrock/data_plane/log/shale/writer.ex`](../../../lib/bedrock/data_plane/log/shale/writer.ex)
- **Recovery Logic**: [`lib/bedrock/data_plane/log/shale/recovery.ex`](../../../lib/bedrock/data_plane/log/shale/recovery.ex)
- **File Management**: [`lib/bedrock/data_plane/log/shale/segment.ex`](../../../lib/bedrock/data_plane/log/shale/segment.ex)
- **Streaming Support**: [`lib/bedrock/data_plane/log/shale/pulling.ex`](../../../lib/bedrock/data_plane/log/shale/pulling.ex)
