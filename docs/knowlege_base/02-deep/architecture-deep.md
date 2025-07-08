# Bedrock Architecture Deep Dive

This document provides a comprehensive architectural reference for Bedrock, combining detailed content from core architectural concepts, transaction processing, and persistent configuration design. This serves as the definitive technical reference for understanding Bedrock's distributed key-value store architecture.

## Table of Contents

1. [Core Architecture Principles](#core-architecture-principles)
2. [Component Architecture](#component-architecture)
3. [Transaction Processing Deep Dive](#transaction-processing-deep-dive)
4. [Multi-Version Concurrency Control (MVCC)](#multi-version-concurrency-control-mvcc)
5. [Persistent Configuration Architecture](#persistent-configuration-architecture)
6. [System Recovery and Bootstrap](#system-recovery-and-bootstrap)
7. [Key Distribution and Sharding](#key-distribution-and-sharding)
8. [Durability and Replication](#durability-and-replication)
9. [Performance Optimizations](#performance-optimizations)
10. [Error Handling and Edge Cases](#error-handling-and-edge-cases)
11. [Testing Strategy](#testing-strategy)
12. [Implementation Status](#implementation-status)
13. [Future Enhancements](#future-enhancements)

## Cross-References

- **Quick References**: [AI Context Quick](../00-quick/ai-context-quick.md) - Core concepts overview
- **Development Support**: [Best Practices](../02-development/best-practices.md) and [Debugging Strategies](../02-development/debugging-strategies.md)
- **Implementation Details**: [Control Plane Components](../03-implementation/control-plane-components.md) and [Data Plane Components](../03-implementation/data-plane-components.md)
- **Complete Reference**: [Bedrock Architecture Livebook](../../docs/bedrock-architecture.livemd) - Comprehensive architectural overview

## Core Architecture Principles

Bedrock follows FoundationDB's separation of concerns with specialized components organized into distinct planes of operation.

### Control Plane vs Data Plane

**Control Plane** (Management and Coordination):
- **Coordinators**: Use Raft consensus to store cluster configuration and elect a Director
- **Director**: Manages system recovery, monitors health, and coordinates the data plane
- **Rate Keeper**: Manages system load and flow control (planned)

**Data Plane** (Transaction Processing):
- **Sequencer**: Assigns global version numbers to transactions
- **Commit Proxies**: Batch transactions and coordinate commits
- **Resolvers**: Implement MVCC conflict detection
- **Logs**: Provide durable transaction storage
- **Storage**: Serve data and follow transaction logs

### Architectural Philosophy

Bedrock embraces a **"let it crash"** philosophy with:
- **Fast Recovery Cycles**: Components fail fast and recover quickly
- **Simple State Management**: Minimal persistent state with clear recovery paths
- **Self-Healing Architecture**: System automatically recovers from failures
- **Comprehensive Testing**: Every recovery path serves as a system test

## Component Architecture

### Coordinator System

The Coordinator system provides the foundation for cluster coordination:

```elixir
# Coordinator startup with persistent state
case Foreman.wait_for_healthy(foreman, timeout: 5_000) do
  :ok ->
    {:ok, workers} = Foreman.storage_workers(foreman)
    read_config_from_storage(workers)
  {:error, :unavailable} ->
    {0, default_config()}  # Fallback to defaults
end
```

**Responsibilities**:
- Raft consensus for leader election
- Cluster configuration management
- Director lifecycle management
- Bootstrap coordination

### Director System

The Director orchestrates system recovery and health monitoring:

```elixir
# Director recovery process
defp run_recovery_process(director_state) do
  with {:ok, services} <- discover_services(director_state),
       {:ok, layout} <- assign_key_ranges(services),
       {:ok, version} <- persist_configuration(layout) do
    {:ok, version}
  else
    {:error, reason} ->
      # Fail fast - exit and let coordinator retry
      exit({:recovery_failed, reason})
  end
end
```

**Responsibilities**:
- Service discovery and health monitoring
- Key range assignment and rebalancing
- System configuration persistence
- Recovery process coordination

### Data Plane Components

#### Sequencer

The Sequencer provides global ordering for transactions:

```elixir
# Version assignment
{:ok, read_version} = Sequencer.get_read_version()
{:ok, commit_version} = Sequencer.get_commit_version()
```

**Responsibilities**:
- Global read version assignment
- Global commit version assignment
- Version ordering guarantees

#### Commit Proxies

Commit Proxies orchestrate the transaction commit process:

```elixir
# Commit proxy workflow
CommitProxy.commit_transaction(%{
  read_version: read_version,
  reads: [keys_and_ranges_read],
  writes: %{key => value, ...}
})
```

**Commit Proxy Workflow**:
1. **Batching**: Collect transactions for batching window
2. **Version Assignment**: Get next commit version from Sequencer
3. **Conflict Resolution**: Send batch to appropriate Resolvers
4. **Conflict Detection**: Resolvers check for conflicts and return abort list
5. **Logging**: Non-conflicting transactions sent to Log servers
6. **Acknowledgment**: Wait for Log durability, then notify clients

#### Resolvers

Resolvers implement MVCC conflict detection:

```elixir
# Conflict resolution
conflicts = Resolver.check_conflicts(batch, %{
  read_keys: transaction.reads,
  write_keys: Map.keys(transaction.writes),
  read_version: transaction.read_version,
  commit_version: batch.commit_version
})
```

**Conflict Rules**:
- Read-Write conflict: Transaction read a key that was written by a later-committed transaction
- Write-Write conflict: Two transactions wrote to the same key
- Within-batch conflicts: Transactions in the same batch conflict with each other

#### Logs

Logs provide durable transaction storage:

```elixir
# Log durability
{:ok, log_version} = Log.append_batch(log_server, committed_transactions)
```

**Responsibilities**:
- Durable transaction storage
- Transaction replay capability
- Storage server coordination

#### Storage

Storage servers provide the final data serving layer:

```elixir
# Storage read at version
{:ok, value} = Storage.fetch(storage_server, key, read_version)
```

**Responsibilities**:
- Multi-version data storage
- Version-specific data serving
- Transaction log following
- Data garbage collection

## Transaction Processing Deep Dive

### Complete Transaction Lifecycle

Bedrock transactions follow a multi-phase lifecycle inspired by FoundationDB:

1. **Initiation**: Client obtains a read version
2. **Read Phase**: Client reads data at the read version
3. **Write Phase**: Client accumulates writes locally
4. **Commit Phase**: Transaction is submitted, resolved, and logged
5. **Completion**: Client is notified of success or failure

### 1. Transaction Initiation

```elixir
# Client code
Repo.transaction(fn repo ->
  # Transaction operations will go here
end)

# Under the hood
{:ok, read_version} = Gateway.get_read_version()
Gateway.lease_read_version(read_version, timeout)
```

When a transaction begins:
- The client contacts the Gateway
- The Gateway requests a read version from the Sequencer
- This read version represents a consistent snapshot of the database
- The Gateway provides a lease for this read version
- All reads in the transaction will use this read version

### 2. Read Phase

```elixir
# Client code
value = Repo.fetch(repo, "customer/123")

# Under the hood
{:ok, value} = Gateway.fetch(read_version, "customer/123")
```

During the read phase:
- The client reads keys using the transaction's read version
- The Gateway tracks all keys and ranges read
- Storage servers provide data at the specified read version
- If a key has been written in the current transaction, the pending write value is returned
- All reads are consistent with the read version snapshot

### 3. Write Phase

```elixir
# Client code
Repo.put(repo, "customer/123/balance", 500)

# Under the hood
Gateway.put("customer/123/balance", 500)
```

During the write phase:
- Writes are accumulated locally in the Gateway
- No network traffic occurs for writes until commit
- The client can read its own writes within the transaction
- The Gateway tracks all keys written

### 4. Commit Phase

```elixir
# Client code (implicit at the end of the transaction block)
# The transaction block returns, triggering commit

# Under the hood
CommitProxy.commit({
  {read_version, read_keys_and_ranges},  # reads
  write_key_values                       # writes
})
```

The commit phase involves several coordinated steps across multiple components.

#### 4.1 Commit Proxy Batching

- The transaction is sent to a Commit Proxy
- The Commit Proxy batches multiple transactions
- Batching continues for a few milliseconds or until batch size limit
- The Commit Proxy requests the next commit version from the Sequencer

#### 4.2 Conflict Resolution

- The batch is sent to Resolvers based on key ranges
- Resolvers check for conflicts:
  - Read-Write conflicts: Transaction read a key that was written by a later-committed transaction
  - Write-Write conflicts: Two transactions wrote to the same key
  - Within-batch conflicts: Transactions in the same batch conflict with each other
- Resolvers return a list of transactions that must be aborted

#### 4.3 Transaction Logging

- Non-conflicting transactions are combined into a single set of changes
- These changes are sent to Log servers for durable storage
- The Commit Proxy waits for acknowledgment from all relevant Log servers
- The commit is assigned the batch's commit version

### 5. Completion

```elixir
# Client code receives result
{:ok, result} = Repo.transaction(fn repo ->
  # Transaction operations
  result
end)

# Or for conflicts
{:error, :conflict} = Repo.transaction(fn repo ->
  # Transaction that had conflicts
end)
```

- Clients with aborted transactions receive a conflict error
- Clients with successful transactions receive success and their result
- The transaction's commit version is available for the client

### Transaction Guarantees

Bedrock transactions provide full ACID guarantees:

- **Atomicity**: All writes commit or none do
- **Consistency**: The database moves from one valid state to another
- **Isolation**: Strict serialization (transactions appear to execute sequentially)
- **Durability**: Committed transactions survive node failures

## Multi-Version Concurrency Control (MVCC)

### Version Management

Bedrock uses several version concepts to manage concurrency:

- **Read Version**: The version at which a transaction reads data
- **Commit Version**: The version assigned when a transaction commits
- **Last Committed Version (LCV)**: The highest committed version in the system
- **Minimum Read Version (MRV)**: The oldest version still needed for active transactions

The system maintains a sliding window between MRV and LCV to efficiently manage MVCC.

### Conflict Resolution Architecture

Resolvers maintain a sliding window of recent transaction activity:

```elixir
# Resolver conflict window management
conflict_window = %{
  start_version: system_mrv,
  end_version: system_lcv,
  read_keys: %{version => [keys...]},
  write_keys: %{version => [keys...]},
  conflict_map: %{key => [versions...]}
}
```

### Conflict Detection Algorithm

The conflict detection process follows these rules:

1. **Read-Write Conflicts**: For each key read by a transaction at read version R, check if any transaction with commit version C (where R < C <= commit_version) wrote to that key.

2. **Write-Write Conflicts**: For each key written by a transaction, check if any other transaction in the same batch or with an overlapping commit version also wrote to that key.

3. **Within-Batch Conflicts**: Check for conflicts between transactions within the same commit batch.

### Storage Version Management

Storage servers maintain sophisticated version management:

```elixir
# Storage server version management
storage_state = %{
  versions: %{
    version1 => %{key => value, ...},
    version2 => %{key => value, ...},
    ...
  },
  min_read_version: mrv,
  last_applied_version: lav
}
```

Storage servers:
- Maintain multiple versions of data
- Serve reads at specific versions
- Follow transaction logs to update their state
- Garbage collect versions older than the system-wide MRV

## Persistent Configuration Architecture

### Design Philosophy

Bedrock uses a self-bootstrapping persistence strategy where:
- **Coordinators** read cluster state from local storage on startup
- **Director** persists cluster state after successful recovery
- **System** uses its own storage infrastructure for persistence

### Core Design Principles

#### 1. Ephemeral Raft + Storage Bootstrap

**Coordinator Raft Logs**: Remain ephemeral (in-memory only)
**Persistent State**: Stored in the system's own key-value storage
**Bootstrap Process**: Coordinators read from storage to initialize Raft state

**Benefits**:
- Raft consensus ensures most recent coordinator wins
- No complex Raft log persistence required
- Leverages existing storage infrastructure
- Natural conflict resolution during startup

#### 2. System Transaction as Comprehensive Test

**Dual Purpose**: The director's system transaction serves as both:
- **Persistence mechanism**: Saves cluster state to storage
- **System test**: Validates entire transaction pipeline

**Fail-Fast Recovery**: If system transaction fails, director exits and coordinator retries with fresh director

**Components Tested**:
- Sequencer (version assignment)
- Commit Proxy (batching and coordination)
- Resolver (conflict detection)
- Logs (durable persistence)
- Storage (eventual application)

#### 3. Self-Healing Architecture

**Fast Recovery Cycles**: Failed system transactions trigger immediate director exit and coordinator retry
**Simple Exponential Backoff**: Coordinator uses simple backoff (1s, 2s, 4s, 8s, max 30s) rather than complex retry logic
**No Partial States**: Either fully operational or clearly failed - no complex circuit breaker logic
**Epoch-Based Recovery**: Each recovery attempt increments epoch counter for generation management

### System Keyspace Layout

#### Reserved Key Prefix
All system keys use the prefix `\xff/system/` to separate them from user data.

#### Key Layout
```
\xff/system/config          -> {epoch, sanitized_cluster_config}
\xff/system/epoch           -> current_epoch_number  
\xff/system/last_recovery   -> recovery_timestamp_ms
```

#### Future Extensions
```
\xff/system/layout/sequencer     -> sequencer_assignment
\xff/system/layout/commit_proxies -> commit_proxy_list
\xff/system/layout/resolvers     -> resolver_assignments
\xff/system/nodes/{node_id}      -> node_capabilities_and_status
```

### Bootstrap Flow Implementation

#### Cold Start (No Existing Storage)

```
1. Coordinator starts
2. Queries foreman for local storage workers
3. No storage found → uses default configuration
4. Raft election with version 0
5. Director starts recovery process
6. Recovery completes → system transaction persists state
7. System ready with persistent configuration
```

#### Warm Start (Existing Storage)

```
1. Coordinator starts
2. Queries foreman for local storage workers
3. Reads \xff/system/config from storage
4. Initializes Raft with storage version
5. Raft election (highest storage version wins)
6. System ready (no recovery needed if already operational)
```

#### Recovery After Failure

```
1. System transaction fails during recovery
2. Director exits with failure reason
3. Coordinator detects director failure
4. Coordinator starts fresh director (new epoch)
5. Recovery process repeats
6. Eventually succeeds or fails obviously
```

### Implementation Details

#### Coordinator Bootstrap

**Storage Discovery**:
```elixir
# Use foreman to find local storage workers
case Foreman.wait_for_healthy(foreman, timeout: 5_000) do
  :ok ->
    {:ok, workers} = Foreman.storage_workers(foreman)
    read_config_from_storage(workers)
  {:error, :unavailable} ->
    {0, default_config()}  # Fallback to defaults
end
```

**Config Reading**:
```elixir
# Read system configuration from storage
case Storage.fetch(storage_worker, "\xff/system/config", :latest) do
  {:ok, bert_data} -> 
    {version, config} = :erlang.binary_to_term(bert_data)
    {version, config}
  {:error, :not_found} -> 
    {0, default_config()}
end
```

#### Director Persistence

**System Transaction Building**:
```elixir
system_transaction = {
  nil,  # reads - system writes don't need reads
  %{    # writes
    "\xff/system/config" => :erlang.term_to_binary({epoch, sanitized_config}),
    "\xff/system/epoch" => :erlang.term_to_binary(epoch),
    "\xff/system/last_recovery" => :erlang.term_to_binary(System.system_time(:millisecond))
  }
}
```

**Direct Commit Proxy Submission**:
```elixir
# Director has direct access to commit proxies it just started
case get_available_commit_proxy(director_state) do
  {:ok, commit_proxy} ->
    case CommitProxy.commit(commit_proxy, system_transaction) do
      {:ok, version} -> 
        # Success - system fully operational
        {:ok, version}
      {:error, reason} ->
        # Fail fast - exit and let coordinator retry
        exit({:recovery_system_test_failed, reason})
    end
end
```

#### Config Sanitization

**Problem**: Cluster config contains PIDs, refs, and other non-serializable data
**Solution**: Sanitize config before BERT encoding

```elixir
defp sanitize_config_for_persistence(config) do
  config
  |> remove_pids_and_refs()
  |> remove_ephemeral_state()
  |> keep_only_persistent_fields()
end
```

## System Recovery and Bootstrap

### Recovery Process Architecture

The recovery process ensures system consistency after startup or failure:

#### Phase 1: Service Discovery
- Director queries all available nodes
- Discovers available services and their capabilities
- Determines cluster topology and health

#### Phase 2: Range Assignment
- Assigns key ranges to Storage servers
- Configures Commit Proxies and Resolvers
- Establishes Log server assignments

#### Phase 3: System Validation
- Executes system transaction to test all components
- Validates entire transaction pipeline
- Persists successful configuration

### Cold Start Process

1. **Coordinator Election**: Raft elects a leader Coordinator
2. **Director Startup**: Leader starts the Director process
3. **Service Discovery**: Director discovers available nodes and services
4. **Range Assignment**: Director assigns key ranges to Storage servers
5. **Log Recovery**: Replay any uncommitted transactions
6. **Service Coordination**: Start Commit Proxies, Resolvers, etc.
7. **System Validation**: Execute system transaction to validate pipeline

### Failure Recovery

- **Node Failure**: Director detects and reassigns services
- **Service Failure**: Automatic restart and state recovery
- **Network Partition**: Raft ensures consistency during splits
- **Transaction Pipeline Failure**: System transaction validates all components

## Key Distribution and Sharding

### Key Space Organization

Bedrock provides a single, continuous, sorted key-value space:
- Keys can be binary or structured (tuples, lists)
- Automatic lexicographic ordering
- Efficient range queries

### Range-Based Sharding

- Key space divided into contiguous ranges
- Each range assigned to specific servers
- Dynamic range splitting and merging (planned)

```elixir
# Example key ranges
ranges = [
  {"", "m"},           # Range 1: keys starting with a-l
  {"m", "z"},          # Range 2: keys starting with m-y  
  {"z", :end}          # Range 3: keys starting with z and beyond
]
```

### Sharding Algorithm

The sharding system uses range-based partitioning:

1. **Range Definition**: Contiguous key ranges with start and end boundaries
2. **Server Assignment**: Each range assigned to one or more Storage servers
3. **Routing**: Requests routed to appropriate servers based on key
4. **Rebalancing**: Automatic range splitting and merging (planned)

### Range Management

```elixir
# Range assignment structure
range_assignment = %{
  range: {start_key, end_key},
  primary_storage: storage_server_id,
  replica_storage: [replica_server_ids...],
  logs: [log_server_ids...],
  resolvers: [resolver_ids...],
  commit_proxies: [commit_proxy_ids...]
}
```

## Durability and Replication

### Transaction Logging

All committed transactions are written to multiple Log servers:
- Provides durability guarantee before client notification
- Enables transaction replay for recovery
- Supports Storage server state reconstruction

### Log Server Architecture

```elixir
# Log server responsibilities
log_server_state = %{
  log_entries: %{version => transaction_batch},
  durability_index: last_durable_version,
  followers: [follower_log_servers...],
  storage_servers: [dependent_storage_servers...]
}
```

### Replication Strategy

- **Transaction Logs**: Replicated across multiple Log servers
- **Storage Data**: Each key range replicated across multiple Storage servers
- **Commit Coordination**: Requires majority of replicas for durability
- **Automatic Failover**: Automatic replica promotion (in development)

### Durability Guarantees

- **Write Durability**: All committed transactions survive node failures
- **Read Consistency**: All reads reflect committed state
- **Recovery Completeness**: System can recover from any failure scenario

## Performance Optimizations

### Batching Strategies

#### Transaction Batching
- Commit Proxies batch multiple transactions
- Reduces per-transaction overhead
- Improves throughput while maintaining latency

#### Log Batching
- Multiple transactions logged together
- Reduces disk I/O operations
- Improves write throughput

### Pipelining

- Multiple transaction phases can overlap
- Read versions assigned while previous transactions commit
- Storage servers can serve reads while processing writes

### Local Storage Utilization

- Storage servers use local disk/memory
- Reduces network latency for local reads
- Enables horizontal scaling of read capacity

### Caching Strategies

#### Version Caching
- Cache recent versions for fast access
- Reduce Storage server load
- Improve read performance

#### Conflict Window Caching
- Resolvers cache recent conflict information
- Accelerate conflict detection
- Reduce memory allocation overhead

## Error Handling and Edge Cases

### Transaction Conflicts

When conflicts occur:
- The client receives a conflict error
- The client can retry the transaction with a new read version
- Exponential backoff is recommended for retries

### Node Failures

- If a node fails during a transaction, the transaction may be aborted
- The Director detects node failures and reassigns services
- Recovery ensures system consistency after failures

### Network Partitions

- Raft consensus ensures correctness during network partitions
- Minority partitions cannot make progress
- Automatic recovery when partitions heal

### Storage Edge Cases

#### Storage Unavailable During Bootstrap
- **Scenario**: Coordinator starts but storage not ready
- **Handling**: Wait briefly for foreman, then fall back to defaults
- **Result**: System starts with default config, will persist after recovery

#### Corrupted Storage Data
- **Scenario**: Storage contains invalid BERT data
- **Handling**: Catch deserialization errors, fall back to defaults
- **Result**: System recovers gracefully, overwrites corrupted data

#### System Transaction Failure
- **Scenario**: Any component in transaction pipeline fails
- **Handling**: Director exits immediately with failure reason
- **Result**: Coordinator detects failure and retries with fresh director

#### Network Partitions During Bootstrap
- **Scenario**: Coordinator can't reach storage on other nodes
- **Handling**: Only reads from local storage workers
- **Result**: Raft consensus ensures correct coordinator wins

## Testing Strategy

### Multi-Layered Testing Approach

#### Unit Tests
- Individual component testing
- Storage discovery with/without available storage
- Config serialization/deserialization
- Error handling for corrupted data

#### Integration Tests
- Component interaction testing
- Cold start scenarios (empty cluster)
- Warm start scenarios (existing storage)
- Failure recovery scenarios (failed system transactions)

#### End-to-End Tests
- Full system testing
- Full cluster restart cycles
- Network partition recovery
- Mixed scenarios (some nodes with storage, some without)

#### Property-Based Testing
- Randomized test scenarios
- Conflict detection correctness
- MVCC property validation
- Recovery process validation

### Transaction Testing Patterns

#### Conflict Testing
```elixir
# Test concurrent transactions with overlapping keys
test "concurrent writes to same key create conflicts" do
  # Start multiple transactions
  # Write to same key
  # Verify conflict detection
  # Ensure only one transaction commits
end
```

#### Version Management Testing
```elixir
# Test MVCC version management
test "read at specific version returns correct data" do
  # Create data at version V1
  # Modify data at version V2
  # Read at V1 should return original data
  # Read at V2 should return modified data
end
```

### Recovery Testing

#### System Transaction Testing
```elixir
# Test system transaction validation
test "system transaction validates entire pipeline" do
  # Start system with various component failures
  # Verify system transaction detects failures
  # Ensure proper recovery cycles
end
```

#### Bootstrap Testing
```elixir
# Test bootstrap scenarios
test "bootstrap with existing storage" do
  # Create storage with configuration
  # Restart system
  # Verify configuration loaded correctly
end
```

## Implementation Status

### Completed Features

- **Persistent Configuration**: Self-bootstrapping cluster state using system's own storage
- **Recovery Process**: Multi-phase director-managed recovery with comprehensive testing
- **Component Structure**: All major control plane and data plane components defined
- **Testing Infrastructure**: Unit, integration, and property-based testing patterns
- **MVCC Foundation**: Version management and conflict detection algorithms
- **Transaction Structure**: Complete transaction data structures and interfaces

### In Development

- **Complete Transaction Flow**: End-to-end transaction processing integration
- **Multi-Node Coordination**: Distributed system coordination and failure handling
- **Performance Optimization**: Throughput and latency improvements
- **Conflict Resolution**: Full implementation of conflict detection algorithms
- **Storage Integration**: Complete Storage server implementation

### Planned Features

- **Deterministic Simulation**: FoundationDB-style comprehensive testing
- **Automatic Sharding**: Dynamic key range management and rebalancing
- **Advanced Monitoring**: Comprehensive observability and performance tracking
- **Rate Limiting**: System load management and flow control
- **Advanced Replication**: Multi-region and advanced replication strategies

## Future Enhancements

### Configuration Management

#### Fine-Grained Configuration
- Move from single config key to multiple specific keys
- Enable incremental updates
- Improve observability and debugging

#### Configuration Versioning
- Add schema versioning for config format
- Enable backward compatibility
- Support config migrations

#### External Configuration
- Support reading initial config from external sources
- Enable configuration management integration
- Support configuration validation

### Performance Enhancements

#### Advanced Batching
- Adaptive batching based on system load
- Priority-based transaction batching
- Latency-sensitive batching algorithms

#### Caching Improvements
- Multi-level caching strategies
- Intelligent cache eviction
- Cross-node cache coordination

#### Network Optimization
- Connection pooling and multiplexing
- Compression for large transactions
- Network topology awareness

### Monitoring and Observability

#### Comprehensive Metrics
- Transaction latency and throughput
- Conflict rates and patterns
- Resource utilization tracking
- Performance bottleneck identification

#### Debugging Tools
- Transaction tracing and visualization
- Conflict analysis tools
- Performance profiling capabilities
- System health dashboards

### Advanced Features

#### Automatic Load Balancing
- Dynamic range rebalancing
- Hot key detection and mitigation
- Adaptive replica placement

#### Multi-Region Support
- Cross-region replication
- Region-aware transaction routing
- Disaster recovery capabilities

## Migration Path

### Phase 1: Basic Persistence (Current)
- Single system config key
- BERT serialization
- Basic error handling
- Core transaction processing

### Phase 2: Enhanced Reliability
- Fine-grained keys
- Better error recovery
- Comprehensive testing
- Performance optimization

### Phase 3: Advanced Features
- Configuration versioning
- External config sources
- Advanced monitoring
- Multi-region support

## Debugging and Monitoring

### Key Metrics

#### System Health
- Bootstrap success/failure rates
- System transaction success/failure rates
- Time to recovery completion
- Component availability and health

#### Transaction Performance
- Transaction latency (read, write, commit)
- Conflict rates and patterns
- Throughput (transactions per second)
- Version management efficiency

#### Resource Utilization
- Storage read/write latencies
- Network bandwidth utilization
- CPU and memory usage patterns
- Disk I/O performance

### Debugging Commands

```elixir
# Check system configuration
Storage.fetch(storage_worker, "\xff/system/config", :latest)

# Check last recovery time
Storage.fetch(storage_worker, "\xff/system/last_recovery", :latest)

# Monitor director recovery progress
GenServer.call(:bedrock_director, :get_state)

# Check transaction statistics
GenServer.call(:bedrock_sequencer, :get_stats)

# Monitor conflict rates
GenServer.call(:bedrock_resolver, :get_conflict_stats)
```

### Common Issues and Solutions

#### Bootstrap Issues
- **Bootstrap fails**: Check foreman health and storage availability
- **Config corruption**: Check BERT serialization/deserialization
- **Storage unavailable**: Verify storage node health and network connectivity

#### Transaction Issues
- **High conflict rates**: Check for hot keys or poor transaction design
- **Slow commits**: Check Log server performance and network latency
- **Read version delays**: Check Sequencer performance and batching

#### Recovery Issues
- **System transaction fails**: Check commit proxy, resolver, and log health
- **Infinite retries**: Check for persistent system issues and dependency problems
- **Partial recovery**: Verify all components are healthy and properly configured

## Comparison to FoundationDB

| Concept | FoundationDB | Bedrock Implementation |
|---------|--------------|----------------------|
| Coordinators | Cluster coordination | `Bedrock.ControlPlane.Coordinator` |
| Cluster Controller | System management | `Bedrock.ControlPlane.Director` |
| Proxies | Transaction processing | `Bedrock.DataPlane.CommitProxy` |
| Resolvers | Conflict detection | `Bedrock.DataPlane.Resolver` |
| Logs | Transaction durability | `Bedrock.DataPlane.Log.Shale` |
| Storage Servers | Data serving | `Bedrock.DataPlane.Storage.Basalt` |
| Sequencer | Version assignment | `Bedrock.DataPlane.Sequencer` |
| Rate Keeper | Flow control | `Bedrock.ControlPlane.RateKeeper` (planned) |

## Contributing to Architecture

When extending Bedrock's architecture:

1. **Follow ACID Principles**: Maintain transaction guarantees
2. **Preserve Separation of Concerns**: Keep control plane and data plane distinct
3. **Implement Fail-Fast**: Components should fail fast and recover quickly
4. **Test Comprehensively**: Include unit, integration, and property-based tests
5. **Document Thoroughly**: Update this document with architectural changes
6. **Consider Performance**: Optimize for both throughput and latency
7. **Plan for Scale**: Design for horizontal scaling and large datasets

This architecture deep dive serves as the definitive reference for understanding Bedrock's design principles, implementation details, and future evolution. Regular updates to this document ensure it remains the authoritative source for architectural decisions and system understanding.