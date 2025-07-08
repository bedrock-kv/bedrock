# Bedrock Implementation Guide

This guide provides consolidated implementation details for all Bedrock components, covering both control plane (cluster coordination) and data plane (transaction processing) components.

## See Also
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md)
- **Recovery System**: [Recovery Internals](../01-architecture/recovery-internals.md)
- **Persistent State**: [Persistent Configuration](../01-architecture/persistent-configuration.md)
- **Development**: [Best Practices](../02-development/best-practices.md) | [Debugging Strategies](../02-development/debugging-strategies.md)
- **Testing**: [Testing Strategies](../02-development/testing-strategies.md) | [Testing Patterns](../02-development/testing-patterns.md)

## System Architecture Overview

Bedrock implements a layered architecture with clear separation between control plane and data plane:

**Control Plane**: Cluster coordination, recovery, and configuration management
- **Coordinator**: Raft consensus and leader election
- **Director**: Recovery orchestration and service management
- **Configuration**: Persistent cluster state and service descriptors

**Data Plane**: Transaction processing and data storage
- **Sequencer**: Global version assignment and ordering
- **Commit Proxy**: Transaction batching and commit coordination
- **Resolver**: MVCC conflict detection
- **Log (Shale)**: Durable transaction storage
- **Storage (Basalt)**: Data serving and version management

## Control Plane Components

### Coordinator - Consensus and Leadership

**Purpose**: Raft consensus for cluster configuration and Director election

**Location**: `lib/bedrock/control_plane/coordinator.ex`

**Key Responsibilities**:
- Raft consensus using `bedrock_raft` dependency
- Single Director election for cluster coordination
- Durable cluster configuration storage
- Node discovery and tracking

**Key Files**:
```elixir
# Main interface and Raft integration
lib/bedrock/control_plane/coordinator.ex
lib/bedrock/control_plane/coordinator/raft_adapter.ex
lib/bedrock/control_plane/coordinator/server.ex
lib/bedrock/control_plane/coordinator/state.ex
```

**Development Patterns**:
```elixir
# Check coordinator status
GenServer.call(:bedrock_coordinator, :get_state)

# Monitor coordinator health
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "coordinator"))
```

### Director - Recovery and Service Management

**Purpose**: System recovery orchestration and data plane service coordination

**Location**: `lib/bedrock/control_plane/director.ex`

**Key Responsibilities**:
- Multi-phase recovery process execution
- Service assignment and health monitoring
- Cluster configuration management
- Node capability tracking

**Recovery Implementation**: The Director implements a strict 6-phase recovery process:

1. **Determining Durable Version** (`recovery/determining_durable_version.ex`)
   - Queries all log servers for highest committed version
   - Establishes recovery baseline

2. **Creating Vacancies** (`recovery/creating_vacancies.ex`)
   - Identifies missing or failed services
   - Determines which workers need creation

3. **Locking Available Services** (`recovery/locking_available_services.ex`)
   - Reserves nodes for specific service roles
   - Prevents assignment conflicts

4. **Filling Vacancies** (`recovery/filling_vacancies.ex`)
   - Creates new workers via direct Foreman contact
   - Assigns specific IDs and registers workers

5. **Defining Core Services** (`recovery/defining_*.ex`)
   - Assigns sequencer, commit proxies, and resolvers
   - Establishes transaction system topology

6. **Replaying Old Logs** (`recovery/replaying_old_logs.ex`)
   - Ensures all committed transactions are applied
   - Brings system to consistent state

**Development Patterns**:
```elixir
# Monitor recovery progress
GenServer.call(:bedrock_director, :get_state)

# Recovery telemetry
:telemetry.attach_many("recovery-monitor", [
  [:bedrock, :director, :recovery, :started],
  [:bedrock, :director, :recovery, :phase_completed],
  [:bedrock, :director, :recovery, :completed]
], fn event, measurements, metadata, config ->
  IO.inspect({event, measurements, metadata}, label: "RECOVERY")
end, nil)
```

### Persistent Configuration System

**Bootstrap Process**: Self-bootstrapping using system's own storage
- Queries local storage workers for existing configuration
- Reads `\xff/system/config` from available storage
- Deserializes BERT-encoded configuration and epoch
- Initializes Raft with storage version
- Falls back to defaults on failure

**Persistence Flow**: System transaction serves dual purpose
- Tests entire data plane pipeline after recovery
- Persists cluster state to `\xff/system/*` keyspace
- Fail-fast behavior triggers restart on failure
- Comprehensive system validation

**Key Files**:
```elixir
# Configuration management
lib/bedrock/control_plane/config.ex
lib/bedrock/control_plane/config/service_descriptor.ex
lib/bedrock/control_plane/config/transaction_system_layout.ex
```

## Data Plane Components

### Sequencer - Global Version Assignment

**Purpose**: Assigns global version numbers for transaction ordering

**Location**: `lib/bedrock/data_plane/sequencer.ex`

**Key Operations**:
```elixir
# Read version for new transactions
{:ok, read_version} = Sequencer.get_read_version()

# Commit version for transaction batches
{:ok, commit_version} = Sequencer.get_commit_version()
```

### Commit Proxy - Transaction Coordination

**Purpose**: Batches transactions and coordinates the commit process

**Location**: `lib/bedrock/data_plane/commit_proxy.ex`

**Commit Process Flow**:
1. Receive transaction from client
2. Add to current batch
3. When batch ready:
   - Get commit version from Sequencer
   - Send batch to Resolvers for conflict detection
   - Send non-conflicting transactions to Logs
   - Notify clients of results

**Key Files**:
```elixir
# Transaction batching and coordination
lib/bedrock/data_plane/commit_proxy/batching.ex
lib/bedrock/data_plane/commit_proxy/batch.ex
lib/bedrock/data_plane/commit_proxy/finalization.ex
```

### Resolver - MVCC Conflict Detection

**Purpose**: Implements MVCC by detecting transaction conflicts

**Location**: `lib/bedrock/data_plane/resolver.ex`

**Conflict Detection Process**:
- Check read keys against write history
- Check write keys against read/write history
- Detect within-batch conflicts
- Return conflicting transaction indices

**MVCC Window Management**:
- Maintains sliding window from MRV to LCV
- Contains read/write sets for all transactions in window
- Efficient conflict tree data structure

**Key Files**:
```elixir
# Conflict detection and MVCC
lib/bedrock/data_plane/resolver/transaction_conflicts.ex
lib/bedrock/data_plane/resolver/tree.ex
lib/bedrock/data_plane/resolver/recovery.ex
```

### Log Component (Shale) - Durable Storage

**Purpose**: Provides durable transaction storage with crash safety

**Location**: `lib/bedrock/data_plane/log/shale.ex`

**File Format**: EOF marker strategy for atomic writes
- Each transaction write includes EOF marker
- Next write overwrites previous EOF marker
- Crash recovery uses EOF marker to find valid data end

**Segment Structure**:
```
[magic_header][transaction1][transaction2]...[eof_marker]
```

**Key Files**:
```elixir
# Log implementation and transactions
lib/bedrock/data_plane/log/shale.ex
lib/bedrock/data_plane/log/transaction.ex
lib/bedrock/data_plane/log/encoded_transaction.ex

# Segment management
lib/bedrock/data_plane/log/shale/segment.ex
lib/bedrock/data_plane/log/shale/segment_recycler.ex

# Push/pull operations
lib/bedrock/data_plane/log/shale/pushing.ex
lib/bedrock/data_plane/log/shale/pulling.ex
lib/bedrock/data_plane/log/shale/long_pulls.ex
```

### Storage Component (Basalt) - Data Serving

**Purpose**: Serves data to clients and maintains local data copies

**Location**: `lib/bedrock/data_plane/storage/basalt.ex`

**Version Management**:
- Maintains multiple versions per key
- Garbage collects versions older than MRV
- Provides consistent read snapshots

**Log Following**:
- Continuously pulls transactions from logs
- Applies transactions in version order
- Updates local data structures

## Service Management Pattern

### Foreman/Worker Architecture

**Pattern**: Each node runs a Foreman that manages local Workers

**Key Files**:
```elixir
# Service management
lib/bedrock/service/foreman.ex
lib/bedrock/service/worker.ex
lib/bedrock/service/worker_behaviour.ex
```

**Development Patterns**:
```elixir
# Check running services
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "bedrock"))

# Visual monitoring
:observer.start()
```

## Version Management System

**Global Version Types**:
- **Read Version**: Snapshot version for reads
- **Commit Version**: Version assigned at commit
- **Last Committed Version (LCV)**: Highest committed version
- **Minimum Read Version (MRV)**: Oldest version still needed

**MRV Advancement Process**:
1. Gateways report minimum active read versions
2. Director aggregates to find global MRV
3. MRV broadcast to all components
4. Components garbage collect versions < MRV

## Testing Patterns

### Unit Testing
```elixir
# Test recovery phases
test "determining durable version finds highest committed version" do
  # Setup mock log servers with different versions
  # Call recovery phase
  # Assert correct version selected
end

# Test conflict detection
test "resolver detects read-write conflict" do
  # Setup conflicting transactions
  # Send to resolver
  # Verify conflict detection
end
```

### Integration Testing
```elixir
# Test complete transaction flow
test "successful transaction commit" do
  # Start transaction
  # Perform reads and writes
  # Commit transaction
  # Verify results
end

# Test log durability
test "log persists transactions across restarts" do
  # Write transactions to log
  # Restart log server
  # Verify transactions available
end
```

## Performance Considerations

**Sequencer**: Version assignment bottleneck under high load
**Commit Proxy**: Balance batch size vs. latency
**Resolver**: CPU-intensive conflict detection, memory usage with window size
**Log**: Disk I/O primary bottleneck, consider SSDs and write batching
**Storage**: Read performance depends on data structure efficiency

## Common Issues and Debugging

**Recovery Issues**:
- Check which recovery phase is failing
- Verify network connectivity and service dependencies
- Monitor resource constraints

**Configuration Issues**:
- Compare configuration across nodes
- Check BERT serialization/deserialization
- Verify configuration propagation

**Performance Issues**:
- Profile components under load
- Monitor memory usage patterns
- Check for network and disk I/O bottlenecks

**Consistency Issues**:
- Verify version ordering across components
- Check MRV advancement logic
- Validate conflict detection accuracy

## Implementation Quick Reference

**Starting Development**:
1. Check [Project Reentry Guide](../00-start-here/project-reentry-guide.md)
2. Review [Development Setup](../00-start-here/development-setup.md)
3. Reference [Quick Reference](../00-start-here/quick-reference.md)

**Common Commands**:
```bash
# Multi-node testing
mix test.multi_node

# Component debugging
iex -S mix
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "bedrock"))

# Recovery monitoring
:telemetry.list_handlers([:bedrock, :director, :recovery])
```

This guide provides the essential implementation details for all Bedrock components. For deeper architectural context, see the comprehensive [Bedrock Architecture Livebook](../../bedrock-architecture.livemd).