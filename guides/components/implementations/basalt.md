# Basalt: Multi-Version Storage Engine

[Basalt](../glossary.md#basalt) is Bedrock's primary [storage](../glossary.md#storage) engine implementation, designed to provide efficient multi-version key-value storage with [MVCC](../glossary.md#multi-version-concurrency-control-mvcc) support. It serves as the persistent data layer that maintains versioned key-value data and continuously applies committed [transactions](../glossary.md#transaction) pulled from the [log](../glossary.md#log) servers.

**Location**: [`lib/bedrock/data_plane/storage/basalt.ex`](../../lib/bedrock/data_plane/storage/basalt.ex)

## Design Philosophy

Basalt is built around the principle that storage servers should optimize for read performance while maintaining perfect consistency with the authoritative transaction log. The engine uses a multi-version approach where each key can have multiple historical values, enabling snapshot isolation and efficient conflict detection support.

The storage engine is designed to handle the eventual consistency model where committed transactions arrive from logs asynchronously but must be applied in strict version order to maintain system-wide consistency.

## Database Architecture

Basalt organizes data using a layered approach that separates fast lookups from durable persistence. The primary storage uses DETS tables for persistence with ETS tables for performance-critical caching.

The multi-version storage maintains a version index that maps each key to an ordered list of versions and their corresponding values. This structure enables efficient version resolution for read operations while supporting garbage collection of old versions.

```elixir
%Database{
  # Primary key-value storage (DETS for persistence)
  storage: :dets_table,
  
  # Version tracking for MVCC
  version_index: %{
    key => [%{version: Bedrock.version(), value: term()}]
  },
  
  # Durability tracking
  durable_version: Bedrock.version(),
  oldest_durable_version: Bedrock.version(),
  
  # Performance optimization
  bloom_filters: %{version => BloomFilter.t()},
  cached_versions: %{key => {version, value}}
}
```

## Transaction Application Process

When Basalt pulls transactions from log servers, it applies them in strict version order to maintain consistency. The application process handles multiple transactions in batches for efficiency while ensuring that version ordering is preserved.

The engine tracks the last applied version and continuously pulls new transactions from assigned log servers. Each transaction contains a set of key-value pairs that are applied atomically at the transaction's version number.

```elixir
def apply_transaction(transaction, database) do
  transaction
  |> Transaction.key_values()
  |> Enum.reduce(database, fn {key, value}, db ->
    Basalt.Database.put(db, key, value, transaction.version)
  end)
end
```

The putting process updates the version index for each affected key, maintaining the ordered list of versions for efficient MVCC reads.

## MVCC Read Implementation

Read operations in Basalt implement true MVCC by finding the latest version of a key that is at or before the requested read version. This ensures that transactions see a consistent snapshot of the data as it existed at their read version.

The version resolution process searches through the version history for each key to find the appropriate version to return. If no version exists at or before the read version, the key is considered not found for that snapshot.

```elixir
def resolve_read_version(key_versions, read_version) do
  key_versions
  |> Enum.filter(fn {version, _value} -> version <= read_version end)
  |> Enum.max_by(fn {version, _value} -> version end, fn -> nil end)
  |> case do
    nil -> {:error, :not_found}
    {version, value} -> {:ok, {version, value}}
  end
end
```

This approach ensures that read operations never see inconsistent or partially committed data, maintaining the isolation guarantees required by the transaction system.

## Performance Optimizations

Basalt includes several performance optimizations designed for the read-heavy workload typical of storage servers. Version caching keeps frequently accessed versions in memory, while bloom filters help quickly eliminate negative lookups.

The storage engine batches transaction applications to amortize the overhead of disk writes and index updates. Range queries are optimized to take advantage of key locality when possible.

For high-traffic keys, Basalt maintains hot caches that bypass the full version resolution process for the most commonly requested versions. This optimization is particularly effective for workloads with temporal locality in read patterns.

## Garbage Collection

Old versions of keys are periodically cleaned up based on the minimum read version tracked by the system. Basalt retains at least one version of each key (the latest) plus any versions that might still be needed by active transactions.

The garbage collection process is designed to be incremental and non-blocking, running in the background while read and write operations continue. It uses the minimum read version information provided by the Director to determine which versions can be safely removed.

```elixir
def garbage_collect_versions(database, minimum_read_version) do
  database.version_index
  |> Enum.map(fn {key, versions} ->
    retained_versions = 
      versions
      |> Enum.filter(fn %{version: v} -> 
        v >= minimum_read_version or is_latest_version?(v, versions)
      end)
    {key, retained_versions}
  end)
  |> Map.new()
end
```

## Key Range Management

Basalt storage servers are assigned specific key ranges and only store data for keys within those ranges. The range assignment is managed by the Director and can change during recovery or load balancing operations.

Range boundaries are checked on every read and write operation to ensure that servers only handle data they're responsible for. This partitioning enables horizontal scaling and load distribution across multiple storage servers.

The engine supports dynamic range splitting, where a busy key range can be divided between multiple storage servers to improve performance and distribute load more evenly.

## Error Handling and Recovery

Basalt uses a fail-fast approach for unrecoverable errors, relying on the Director to coordinate recovery when needed. Recoverable errors like temporary disk issues are handled with retry logic and graceful degradation.

During recovery, Basalt can rebuild its state from the transaction logs, ensuring that no committed data is lost even after complete storage server failures. The recovery process verifies data consistency and reports any inconsistencies to the Director.

The engine includes continuous data validation to detect corruption early and prevent serving invalid data to the transaction system.

## Monitoring and Telemetry

Basalt provides extensive telemetry about its operation, including read throughput, storage utilization, version distribution, and cache performance. This information helps operators understand system health and performance characteristics.

The engine tracks key metrics like read latency percentiles, cache hit rates, and garbage collection efficiency. It also monitors the transaction pulling process to ensure storage servers stay current with the log servers.

```elixir
[:bedrock, :storage, :fetch_completed]       # Read operation completed
[:bedrock, :storage, :transaction_applied]   # Transaction applied from log
[:bedrock, :storage, :garbage_collection]    # Version cleanup performed
[:bedrock, :storage, :recovery_completed]    # Recovery process finished
```

## Configuration and Tuning

Basalt's configuration focuses on balancing read performance, storage efficiency, and resource usage. Database paths, cache sizes, and version retention policies can be tuned for specific workloads and hardware characteristics.

Performance tuning options include transaction application batch sizes, garbage collection frequency, and caching strategies. The engine provides reasonable defaults while allowing operators to optimize for their specific deployment requirements.

## Related Components

- **[Storage System](../data-plane/storage.md)**: General storage system concepts and interface
- **[Log System](../data-plane/log.md)**: Source of committed transactions for Basalt
- **[Transaction Builder](../control-plane/transaction-builder.md)**: Primary consumer of Basalt read operations
- **Director**: Control plane component that manages Basalt recovery and key range assignment

## Code References

- **Main Implementation**: [`lib/bedrock/data_plane/storage/basalt.ex`](../../lib/bedrock/data_plane/storage/basalt.ex)
- **Database Engine**: [`lib/bedrock/data_plane/storage/basalt/database.ex`](../../lib/bedrock/data_plane/storage/basalt/database.ex)
- **Server Logic**: [`lib/bedrock/data_plane/storage/basalt/server.ex`](../../lib/bedrock/data_plane/storage/basalt/server.ex)
- **Transaction Pulling**: [`lib/bedrock/data_plane/storage/basalt/pulling.ex`](../../lib/bedrock/data_plane/storage/basalt/pulling.ex)
- **MVCC Logic**: [`lib/bedrock/data_plane/storage/basalt/multi_version_concurrency_control.ex`](../../lib/bedrock/data_plane/storage/basalt/multi_version_concurrency_control.ex)