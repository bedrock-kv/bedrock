# Basalt

[Basalt](../../../glossary.md#basalt) is Bedrock's primary [storage](../../../glossary.md#storage) engine implementation, designed to provide efficient multi-version key-value storage with [MVCC](../../../glossary.md#multi-version-concurrency-control) support. It serves as the persistent data layer that maintains versioned key-value data and continuously applies committed [transactions](../../../glossary.md#transaction) pulled from the [log](../../../glossary.md#log) servers.

**Location**: [`lib/bedrock/data_plane/storage/basalt/`](../../../lib/bedrock/data_plane/storage/basalt/)

## Design Philosophy

Basalt is built around the principle that storage servers should optimize for read performance while maintaining perfect consistency with the authoritative transaction log. The engine uses a multi-version approach where each key can have multiple historical values, enabling snapshot isolation and efficient conflict detection support.

The storage engine is designed to handle the eventual consistency model where committed transactions arrive from logs asynchronously but must be applied in strict version order to maintain system-wide consistency.

## Database Architecture

Basalt organizes data using three primary components that work together to provide MVCC storage with efficient persistence and performance:

### Core Components

The **MVCC component** uses an ETS ordered_set table to store versioned key-value pairs as tuples in the format `{versioned_key(key, version), value}`. This structure enables efficient version-based lookups by leveraging ETS's ordered set properties to find the latest version at or before a requested read version.

**Keyspace management** tracks which keys exist in the system to optimize negative lookups. When a key doesn't exist in the in-memory MVCC data, the keyspace component can quickly determine whether the key exists at all before attempting expensive persistence lookups.

**PersistentKeyValues** provides durable storage using DETS tables for crash recovery and long-term data retention. This component stores older versions that have been aged out of memory but are still within the durability retention window.

The system uses a configurable **window_size_in_microseconds** parameter for batching durability operations, balancing write performance with data safety guarantees.

## Transaction Application Process

Basalt continuously pulls transactions from log servers through its pulling mechanism and applies them in strict version order to maintain consistency. The application process uses the MVCC component to store versioned data efficiently.

### Transaction Ordering and Application

Transactions must be applied in strict version order to maintain system-wide consistency. The system enforces this by checking that each incoming transaction has a version greater than the current newest version before application. Any attempt to apply transactions out of order results in an immediate failure to prevent data corruption.

Each transaction consists of a version number and a set of key-value pairs. During application, the system creates versioned keys using a tuple structure that combines the original key with its version, enabling efficient MVCC lookups. The newest version marker is updated atomically with the transaction data to maintain consistency.

The pulling process runs in a separate task, continuously fetching transaction batches from available log servers. This design ensures that storage servers remain synchronized with the authoritative transaction log while allowing for efficient batch processing to improve performance.

## MVCC Read Implementation

Read operations in Basalt implement true MVCC by finding the latest version of a key at or before the requested read version. The implementation uses ETS select operations for efficient version resolution.

### Three-Tier Lookup Strategy

Basalt employs a sophisticated three-tier lookup strategy to optimize read performance while maintaining data consistency:

**Tier 1 - MVCC Memory Lookup**: The system first searches the in-memory MVCC table using ETS select_reverse operations. This leverages the ordered set structure to efficiently find the latest version of a key that is less than or equal to the requested read version. The versioned key tuple structure enables this lookup to be performed with a single ETS operation.

**Tier 2 - Keyspace Verification**: If the key is not found in memory, the system checks the Keyspace component to determine if the key exists at all in the system. This prevents expensive persistence lookups for keys that have never been written.

**Tier 3 - Persistence Fallback**: For keys that exist but aren't in memory, the system performs a persistence lookup and writes the result back to the MVCC cache for future access. This tier also handles the `:version_too_old` error case when the requested version is older than the system's current retention window.

This approach minimizes expensive disk operations while ensuring that all valid data remains accessible even when memory constraints require aging out older versions.

## Performance Optimizations

Basalt includes several performance optimizations designed for the read-heavy workload typical of storage servers.

### ETS-Based Optimizations

The storage engine leverages ETS (Erlang Term Storage) ordered sets to provide highly efficient key-value operations. The versioned key tuple structure allows for rapid version resolution using ETS's built-in ordering capabilities, eliminating the need for complex indexing structures.

Version caching keeps frequently accessed versions in memory, reducing the need for persistence lookups. The system uses intelligent cache eviction policies that consider both access frequency and version age to maintain optimal memory utilization.

### Batch Processing

The storage engine batches transaction applications to amortize the overhead of disk writes and index updates. This approach significantly improves throughput for high-volume transaction streams while maintaining the strict ordering requirements of MVCC.

For high-traffic keys, Basalt maintains optimized access patterns that take advantage of temporal locality in read patterns, ensuring that recently accessed data remains readily available in the fastest storage tiers.

## Durability and Version Management

Basalt manages durability through a time-windowed approach that balances performance with data safety. The system maintains both in-memory MVCC data and persistent storage, with periodic flushing based on configurable time windows.

### Time-Based Durability Windows

The durability system operates on the principle of time-based version windows measured in microseconds. The system calculates a "trailing edge" version by subtracting the configured window size from the newest version timestamp. Any versions older than this trailing edge are candidates for persistence and potential memory cleanup.

Durability operations are triggered when the oldest version in memory falls behind the calculated trailing edge. This ensures that recent data remains in fast memory while older data is safely persisted to durable storage.

### Version Cleanup Process

Old versions are automatically pruned when they fall outside the durability window, ensuring efficient memory usage while maintaining transaction isolation guarantees. The cleanup process uses ETS select_delete operations to efficiently remove multiple versions in a single operation, minimizing the performance impact of version management.

The system maintains explicit tracking of the oldest and newest versions in memory, enabling efficient boundary checking for both durability operations and version cleanup processes.

## Error Handling and Circuit Breaker

Basalt uses a fail-fast approach for unrecoverable errors, relying on the Director to coordinate recovery when needed. The system includes sophisticated circuit breaker mechanisms for handling temporary failures in log server connectivity.

### Circuit Breaker Behavior

When log pull operations fail, Basalt employs a circuit breaker pattern to prevent overwhelming failed log servers with retry attempts. The circuit breaker tracks failure rates and implements exponential backoff with configurable wait times.

Once a log server is marked as failed, the circuit breaker prevents further attempts for a cooling-off period. The system automatically resets the circuit breaker and resumes pull attempts when the wait period expires, enabling automatic recovery from transient network or server issues.

### Recovery Process

During recovery, Basalt can rebuild its state from the transaction logs, ensuring that no committed data is lost even after complete storage server failures. The recovery process verifies data consistency and reports any inconsistencies to the Director.

The engine includes continuous data validation to detect corruption early and prevent serving invalid data to the transaction system.

## Monitoring and Telemetry

Basalt provides comprehensive telemetry focused on the transaction pulling process, which is critical for maintaining consistency with the authoritative log servers. The telemetry events track pull operations, failures, and circuit breaker behavior.

### Telemetry Events

The system emits the following telemetry events to enable comprehensive monitoring:

**Pull Operation Events**:

- `[:bedrock, :storage, :pull_start]` - Log pull operation started
- `[:bedrock, :storage, :pull_succeeded]` - Log pull completed successfully  
- `[:bedrock, :storage, :pull_failed]` - Log pull encountered error

**Circuit Breaker Events**:

- `[:bedrock, :storage, :log_marked_as_failed]` - Log server marked as unavailable
- `[:bedrock, :storage, :log_pull_circuit_breaker_tripped]` - Circuit breaker activated
- `[:bedrock, :storage, :log_pull_circuit_breaker_reset]` - Circuit breaker reset

These events include metadata such as transaction counts, timestamps, failure reasons, and circuit breaker wait times, enabling operators to monitor the health of the storage system's integration with the log infrastructure.

## Configuration and Tuning

Basalt's configuration focuses on balancing read performance, storage efficiency, and resource usage. Database paths, cache sizes, and version retention policies can be tuned for specific workloads and hardware characteristics.

Performance tuning options include transaction application batch sizes, garbage collection frequency, and caching strategies. The engine provides reasonable defaults while allowing operators to optimize for their specific deployment requirements.

## Key Components

Basalt consists of several specialized modules that work together to provide MVCC storage:

- **Database**: Coordinates MVCC, Keyspace, and PersistentKeyValues components
- **MVCC**: Manages versioned data in ETS tables with efficient lookups
- **Keyspace**: Tracks key existence to optimize negative lookups
- **PersistentKeyValues**: Provides DETS-based durable storage
- **Pulling**: Continuously fetches transactions from log servers
- **Server**: GenServer implementation handling client requests and recovery
- **Telemetry**: Monitoring and observability for pull operations

## Related Components

- **[Storage System](../data-plane/storage.md)**: General storage system concepts and interface
- **[Log System](../data-plane/log.md)**: Source of committed transactions for Basalt
- **[Transaction Builder](../infrastructure/transaction-builder.md)**: Primary consumer of Basalt read operations
- **[Director](../control-plane/director.md)**: Control plane component that manages Basalt recovery and key range assignment

## Code References

- **Main Module**: [`lib/bedrock/data_plane/storage/basalt.ex`](../../../lib/bedrock/data_plane/storage/basalt.ex)
- **Database Engine**: [`lib/bedrock/data_plane/storage/basalt/database.ex`](../../../lib/bedrock/data_plane/storage/basalt/database.ex)
- **MVCC Implementation**: [`lib/bedrock/data_plane/storage/basalt/multi_version_concurrency_control.ex`](../../../lib/bedrock/data_plane/storage/basalt/multi_version_concurrency_control.ex)
- **Keyspace Management**: [`lib/bedrock/data_plane/storage/basalt/keyspace.ex`](../../../lib/bedrock/data_plane/storage/basalt/keyspace.ex)
- **Persistent Storage**: [`lib/bedrock/data_plane/storage/basalt/persistent_key_values.ex`](../../../lib/bedrock/data_plane/storage/basalt/persistent_key_values.ex)
- **Transaction Pulling**: [`lib/bedrock/data_plane/storage/basalt/pulling.ex`](../../../lib/bedrock/data_plane/storage/basalt/pulling.ex)
- **Server Logic**: [`lib/bedrock/data_plane/storage/basalt/server.ex`](../../../lib/bedrock/data_plane/storage/basalt/server.ex)
- **Telemetry**: [`lib/bedrock/data_plane/storage/basalt/telemetry.ex`](../../../lib/bedrock/data_plane/storage/basalt/telemetry.ex)
