# Data Plane Components Implementation Guide

This guide covers the implementation details of Bedrock's data plane components, which handle transaction processing and data storage.

## See Also
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md) - Core architectural principles and component relationships
- **Transaction Flow**: [Transaction Lifecycle](../01-architecture/transaction-lifecycle.md) - End-to-end transaction processing
- **Development Support**: [Best Practices](../02-development/best-practices.md) and [Debugging Strategies](../02-development/debugging-strategies.md)
- **Testing Approaches**: [Testing Strategies](../02-development/testing-strategies.md) and [Testing Patterns](../02-development/testing-patterns.md)
- **Control Plane**: [Control Plane Components](control-plane-components.md) - Cluster coordination and management

## Overview

The data plane is responsible for:
- Transaction processing and coordination
- Multi-version concurrency control (MVCC)
- Durable transaction logging
- Data storage and retrieval
- Version management

## Sequencer Component

### Purpose
The Sequencer assigns global version numbers to ensure transaction ordering.

### Implementation Location
- `lib/bedrock/data_plane/sequencer.ex`
- `lib/bedrock/data_plane/sequencer/`

### Key Responsibilities
1. **Read Version Assignment**: Provides read versions for new transactions
2. **Commit Version Assignment**: Assigns commit versions to transaction batches
3. **Version Ordering**: Ensures global ordering of all transactions
4. **Version Tracking**: Maintains current version state

### Key Files and Functions

```elixir
# Main sequencer interface
lib/bedrock/data_plane/sequencer.ex

# Server implementation
lib/bedrock/data_plane/sequencer/server.ex

# State management
lib/bedrock/data_plane/sequencer/state.ex
```

### Development Patterns

#### Getting Read Versions
```elixir
# Request a read version for a new transaction
{:ok, read_version} = Sequencer.get_read_version()

# The read version represents a consistent snapshot
```

#### Getting Commit Versions
```elixir
# Commit proxies request commit versions for batches
{:ok, commit_version} = Sequencer.get_commit_version()
```

### Common Issues
- **Version Gaps**: Ensure sequencer state is properly persisted
- **Performance Bottlenecks**: Sequencer can become a bottleneck under high load
- **Consistency Issues**: Version assignment must be atomic

## Commit Proxy Component

### Purpose
The Commit Proxy batches transactions, coordinates conflict resolution, and manages the commit process.

### Implementation Location
- `lib/bedrock/data_plane/commit_proxy.ex`
- `lib/bedrock/data_plane/commit_proxy/`

### Key Responsibilities
1. **Transaction Batching**: Groups transactions for efficient processing
2. **Conflict Resolution Coordination**: Sends batches to resolvers
3. **Log Coordination**: Ensures transactions are durably logged
4. **Client Notification**: Notifies clients of commit results

### Key Files and Functions

```elixir
# Main commit proxy interface
lib/bedrock/data_plane/commit_proxy.ex

# Batching logic
lib/bedrock/data_plane/commit_proxy/batching.ex

# Batch data structure
lib/bedrock/data_plane/commit_proxy/batch.ex

# Finalization process
lib/bedrock/data_plane/commit_proxy/finalization.ex

# Server implementation
lib/bedrock/data_plane/commit_proxy/server.ex
```

### Development Patterns

#### Commit Process Flow
```elixir
# 1. Receive transaction from client
# 2. Add to current batch
# 3. When batch is full or timeout reached:
#    a. Get commit version from Sequencer
#    b. Send batch to Resolvers for conflict detection
#    c. Send non-conflicting transactions to Logs
#    d. Notify clients of results
```

#### Batch Management
```elixir
# Batches are created with configurable parameters:
# - Maximum batch size
# - Maximum batch timeout
# - Priority handling
```

### Common Issues
- **Batch Timeouts**: Balance between latency and throughput
- **Resolver Coordination**: Handle resolver failures gracefully
- **Log Durability**: Ensure all logs acknowledge before client notification

## Resolver Component

### Purpose
The Resolver implements MVCC by detecting transaction conflicts.

### Implementation Location
- `lib/bedrock/data_plane/resolver.ex`
- `lib/bedrock/data_plane/resolver/`

### Key Responsibilities
1. **Conflict Detection**: Identifies read-write and write-write conflicts
2. **MVCC Window Management**: Maintains sliding window of transaction history
3. **Batch Processing**: Processes transaction batches from commit proxies
4. **Conflict Tree Management**: Efficiently tracks key access patterns

### Key Files and Functions

```elixir
# Main resolver interface
lib/bedrock/data_plane/resolver.ex

# Conflict detection logic
lib/bedrock/data_plane/resolver/transaction_conflicts.ex

# Conflict tree data structure
lib/bedrock/data_plane/resolver/tree.ex

# Server implementation
lib/bedrock/data_plane/resolver/server.ex

# Recovery logic
lib/bedrock/data_plane/resolver/recovery.ex
```

### Development Patterns

#### Conflict Detection Process
```elixir
# For each transaction in a batch:
# 1. Check read keys against write history
# 2. Check write keys against read/write history
# 3. Check for within-batch conflicts
# 4. Return list of conflicting transaction indices
```

#### MVCC Window Management
```elixir
# The resolver maintains a sliding window:
# - Start: Minimum Read Version (MRV)
# - End: Last Committed Version (LCV)
# - Contains: Read/write sets for all transactions in window
```

### Common Issues
- **Memory Usage**: Conflict window can grow large under high load
- **Performance**: Conflict detection must be very fast
- **Window Management**: Proper MRV advancement is critical

## Log Component (Shale)

### Purpose
The Log component provides durable storage for committed transactions.

### Implementation Location
- `lib/bedrock/data_plane/log.ex`
- `lib/bedrock/data_plane/log/shale.ex`
- `lib/bedrock/data_plane/log/shale/`

### Key Responsibilities
1. **Durable Storage**: Persist transactions to disk
2. **Replication**: Maintain multiple copies for fault tolerance
3. **Segment Management**: Organize data into manageable segments
4. **Recovery**: Replay transactions during system recovery

### File Format and Crash Safety

**EOF Marker Strategy**: Implements atomic write safety using EOF markers:
- Each transaction write includes an EOF marker
- Next write overwrites the previous EOF marker
- If crash occurs, EOF marker indicates end of valid data
- Provides crash safety without complicating the format of the transaction logs

**Segment Structure**: Log segments are organized as:
```
[magic_header][transaction1][transaction2]...[eof_marker]
```
Each transaction contains: version + size + payload + CRC32

### Key Files and Functions

```elixir
# Main log interface
lib/bedrock/data_plane/log.ex

# Shale log implementation
lib/bedrock/data_plane/log/shale.ex

# Transaction data structures
lib/bedrock/data_plane/log/transaction.ex
lib/bedrock/data_plane/log/encoded_transaction.ex

# Segment management
lib/bedrock/data_plane/log/shale/segment.ex
lib/bedrock/data_plane/log/shale/segment_recycler.ex

# Push/pull operations
lib/bedrock/data_plane/log/shale/pushing.ex
lib/bedrock/data_plane/log/shale/pulling.ex
lib/bedrock/data_plane/log/shale/long_pulls.ex

# Recovery operations
lib/bedrock/data_plane/log/shale/recovery.ex
lib/bedrock/data_plane/log/shale/cold_starting.ex
```

### Development Patterns

#### Lazy Loading Design
Segments use lazy loading for memory efficiency:
- `segment.transactions = nil` by design (not loaded)
- `load_transactions/1` loads from disk on-demand
- Prevents memory bloat with large numbers of segments

#### Transaction Storage
```elixir
# Transactions are stored in segments:
# - Each segment has a version range
# - Segments are immutable once written
# - Old segments can be garbage collected
```

#### Push/Pull Model
```elixir
# Storage servers pull transactions from logs:
# - Long polling for efficiency
# - Batch pulling for throughput
# - Version-based synchronization
```

### Common Issues
- **Disk Performance**: Log writes must be fast and durable
- **Segment Management**: Balance segment size vs. overhead
- **Recovery Time**: Large logs can slow recovery

## Storage Component (Basalt)

### Purpose
The Storage component serves data to clients and maintains local copies of data.

### Implementation Location
- `lib/bedrock/data_plane/storage.ex`
- `lib/bedrock/data_plane/storage/basalt.ex`
- `lib/bedrock/data_plane/storage/basalt/`

### Key Responsibilities
1. **Data Serving**: Respond to client read requests
2. **Version Management**: Maintain multiple versions of data
3. **Log Following**: Pull and apply transactions from logs
4. **Garbage Collection**: Remove old versions when safe

### Key Files and Functions

```elixir
# Main storage interface
lib/bedrock/data_plane/storage.ex

# Basalt storage implementation
lib/bedrock/data_plane/storage/basalt.ex

# Storage-specific implementations in basalt/ subdirectory
```

### Development Patterns

#### Version Storage
```elixir
# Storage maintains multiple versions:
# - Each key can have multiple version entries
# - Versions older than MRV can be garbage collected
# - Reads specify the desired version
```

#### Log Following
```elixir
# Storage servers continuously pull from logs:
# - Apply transactions in version order
# - Update local data structures
# - Advance local version pointers
```

### Common Issues
- **Memory Usage**: Multiple versions can consume significant memory
- **Read Performance**: Version lookup must be efficient
- **Consistency**: Ensure proper version ordering

## Version Management

### Global Version Concepts

```elixir
# Key version types in the system:
# - Read Version: Snapshot version for reads
# - Commit Version: Version assigned at commit
# - Last Committed Version (LCV): Highest committed version
# - Minimum Read Version (MRV): Oldest version still needed
```

### Implementation Location
- `lib/bedrock/data_plane/version.ex`

### Development Patterns

#### Version Advancement
```elixir
# MRV advancement process:
# 1. Gateways report their minimum active read versions
# 2. Director aggregates to find global MRV
# 3. MRV is broadcast to all components
# 4. Components can garbage collect versions < MRV
```

## Testing Data Plane Components

### Unit Testing
- Test individual component logic in isolation
- Mock dependencies for focused testing
- Use property-based testing for conflict detection

### Integration Testing
- Test complete transaction flows
- Simulate various failure scenarios
- Test version management across components

### Performance Testing
- Measure transaction throughput
- Test conflict detection performance
- Benchmark log write performance

### Common Test Patterns

```elixir
# Test transaction commit flow
test "successful transaction commit" do
  # Start transaction
  # Perform reads and writes
  # Commit transaction
  # Verify results
end

# Test conflict detection
test "resolver detects read-write conflict" do
  # Setup conflicting transactions
  # Send to resolver
  # Verify conflict detection
end

# Test log durability
test "log persists transactions across restarts" do
  # Write transactions to log
  # Restart log server
  # Verify transactions are still available
end
```

## Performance Considerations

### Sequencer Performance
- Version assignment must be very fast
- Consider batching version requests
- Monitor for bottlenecks under high load

### Commit Proxy Performance
- Batch size affects both latency and throughput
- Resolver coordination can be parallelized
- Log coordination requires careful timeout management

### Resolver Performance
- Conflict detection is CPU-intensive
- Memory usage grows with conflict window size
- Consider partitioning by key ranges

### Log Performance
- Disk I/O is the primary bottleneck
- Use appropriate storage hardware (SSDs)
- Consider write batching for throughput

### Storage Performance
- Read performance depends on data structure efficiency
- Memory usage grows with version count
- Local storage reduces network latency

## Debugging Tips

### Transaction Flow Issues
1. Trace transactions through each component
2. Check version assignments and ordering
3. Verify conflict detection logic
4. Monitor log write performance

### Performance Issues
1. Profile each component under load
2. Monitor memory usage patterns
3. Check for network bottlenecks
4. Analyze disk I/O patterns

### Consistency Issues
1. Verify version ordering across components
2. Check MRV advancement logic
3. Ensure proper transaction isolation
4. Validate conflict detection accuracy
