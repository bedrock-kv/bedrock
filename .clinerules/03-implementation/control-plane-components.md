# Control Plane Components Implementation Guide

This guide covers the implementation details of Bedrock's control plane components, which handle cluster coordination and management.

## See Also
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md) - Core architectural principles and component relationships
- **Persistent State**: [Persistent Configuration](../01-architecture/persistent-configuration.md) - Self-bootstrapping cluster state design
- **Development Support**: [Best Practices](../02-development/best-practices.md) and [Debugging Strategies](../02-development/debugging-strategies.md)
- **Testing Approaches**: [Testing Strategies](../02-development/testing-strategies.md) and [Testing Patterns](../02-development/testing-patterns.md)
- **Data Plane**: [Data Plane Components](data-plane-components.md) - Transaction processing components

## Overview

The control plane is responsible for:
- Cluster coordination and consensus
- System recovery and health monitoring
- Configuration management
- Service discovery and assignment

## Coordinator Component

### Purpose
The Coordinator uses Raft consensus to maintain cluster configuration and elect a Director.

### Implementation Location
- `lib/bedrock/control_plane/coordinator.ex`
- `lib/bedrock/control_plane/coordinator/`

### Key Responsibilities
1. **Raft Consensus**: Implements distributed consensus using the `bedrock_raft` dependency
2. **Leader Election**: Elects a single Director for the cluster
3. **Configuration Storage**: Durably stores cluster configuration
4. **Node Discovery**: Tracks available nodes in the cluster

### Key Files and Functions

```elixir
# Main coordinator interface
lib/bedrock/control_plane/coordinator.ex

# Raft integration
lib/bedrock/control_plane/coordinator/raft_adapter.ex

# Server implementation
lib/bedrock/control_plane/coordinator/server.ex

# State management
lib/bedrock/control_plane/coordinator/state.ex
```

### Development Patterns

#### Starting a Coordinator
```elixir
# Coordinators are started as part of the cluster supervision tree
# Check lib/bedrock/internal/cluster_supervisor.ex for startup logic
```

#### Checking Coordinator Status
```elixir
# In IEx, check if coordinator is running
GenServer.call(:bedrock_coordinator, :get_state)

# Check Raft leader status
# Implementation depends on bedrock_raft integration
```

### Common Issues
- **Split Brain**: Ensure exactly 3 nodes for proper quorum
- **Leader Election Failures**: Check network connectivity between nodes
- **Configuration Conflicts**: Verify all nodes have consistent configuration

## Director Component

### Purpose
The Director manages system recovery, monitors cluster health, and coordinates the data plane.

### Implementation Location
- `lib/bedrock/control_plane/director.ex`
- `lib/bedrock/control_plane/director/`

### Key Responsibilities
1. **System Recovery**: Orchestrates cluster recovery after failures
2. **Service Management**: Assigns and monitors data plane services
3. **Health Monitoring**: Tracks node and service health
4. **Configuration Updates**: Manages cluster configuration changes

### Recovery Process Implementation

The Director implements a multi-phase recovery process where each phase must complete before the next begins:

#### Phase 1: Determining Durable Version
```elixir
# lib/bedrock/control_plane/director/recovery/determining_durable_version.ex
# Finds the highest committed version across all log servers
```

#### Phase 2: Creating Vacancies
```elixir
# lib/bedrock/control_plane/director/recovery/creating_vacancies.ex
# Identifies which services need to be started or restarted
```

#### Phase 3: Locking Available Services
```elixir
# lib/bedrock/control_plane/director/recovery/locking_available_services.ex
# Reserves nodes for specific service roles
```

#### Phase 4: Filling Vacancies (Worker Creation)
The Director orchestrates on-demand worker creation:
- Determines placement across available nodes
- Contacts Foreman on target nodes directly (avoiding Director API deadlocks)
- Creates workers with specific IDs
- Registers new workers in available services

```elixir
# lib/bedrock/control_plane/director/recovery/filling_vacancies.ex
# Creates missing workers when needed, not just assigns existing ones
```

#### Phase 5: Defining Core Services
```elixir
# lib/bedrock/control_plane/director/recovery/defining_sequencer.ex
# lib/bedrock/control_plane/director/recovery/defining_commit_proxies.ex
# lib/bedrock/control_plane/director/recovery/defining_resolvers.ex
# Assigns sequencer, commit proxies, and resolvers
```

#### Phase 6: Replaying Old Logs
```elixir
# lib/bedrock/control_plane/director/recovery/replaying_old_logs.ex
# Ensures all committed transactions are applied
```

### Development Patterns

#### Monitoring Recovery Progress
```elixir
# Check Director state
GenServer.call(:bedrock_director, :get_state)

# Look for recovery telemetry events
:telemetry.attach_many(
  "recovery-monitor",
  [
    [:bedrock, :director, :recovery, :started],
    [:bedrock, :director, :recovery, :phase_completed],
    [:bedrock, :director, :recovery, :completed]
  ],
  fn event, measurements, metadata, config ->
    IO.inspect({event, measurements, metadata}, label: "RECOVERY")
  end,
  nil
)
```

#### Adding New Recovery Phases
1. Create a new module in `lib/bedrock/control_plane/director/recovery/`
2. Implement the phase logic
3. Add the phase to the recovery sequence in `recovery.ex`
4. Add appropriate telemetry events

### Node Tracking

The Director tracks available nodes and their capabilities:

```elixir
# lib/bedrock/control_plane/director/node_tracking.ex
# Monitors which nodes are available and what services they can run

# lib/bedrock/control_plane/director/nodes.ex
# Manages node state and capabilities
```

### Coordinator Bootstrap with Persistent Configuration

The Coordinator now supports bootstrapping from persistent storage, enabling warm starts and cluster state continuity across restarts.

**Key Changes:**
- Reads system configuration from local storage workers on startup
- Uses foreman to discover available storage workers
- Falls back to default configuration if no storage available
- Initializes Raft with storage-derived version (not 0)

**Bootstrap Flow:**
1. Query foreman for local storage workers with timeout
2. Read `\xff/system/config` from available storage
3. Deserialize BERT-encoded configuration and epoch
4. Initialize Raft with storage version as starting point
5. Fall back to defaults on any failure

**Error Handling:**
- Graceful fallback when storage unavailable
- BERT deserialization error recovery
- Timeout handling for foreman queries
- Corrupted data detection and recovery

**Benefits:**
- Coordinators with existing storage win Raft elections
- System state persists across full cluster restarts
- Natural conflict resolution during startup
- No complex Raft log persistence required

### Director System State Persistence

The Director persists cluster state after successful recovery using a system transaction that serves dual purposes: persistence and comprehensive system testing.

**Key Concepts:**
- System transaction tests entire data plane pipeline
- Direct submission to commit proxy (bypassing gateway)
- Fail-fast behavior: transaction failure triggers director restart
- Uses system keyspace (`\xff/system/*`) for cluster state

**Persistence Flow:**
1. Recovery completes successfully
2. Director builds system transaction with cluster state
3. Submits directly to available commit proxy
4. Transaction tests: Sequencer → Commit Proxy → Resolver → Logs → Storage
5. Success indicates fully operational system
6. Failure triggers director exit and coordinator retry

**System Transaction Contents:**
- `\xff/system/config`: Complete cluster configuration (BERT-encoded)
- `\xff/system/epoch`: Current epoch number
- `\xff/system/last_recovery`: Recovery completion timestamp

**Error Handling:**
- Transaction failure causes director to exit immediately
- Coordinator detects director failure and starts fresh director
- New director attempts recovery with incremented epoch
- System eventually converges to stable state or fails obviously

**Benefits:**
- Comprehensive system validation after recovery
- Automatic persistence of cluster state
- Self-healing through fail-fast behavior
- No partial recovery states to debug

### Common Issues
- **Recovery Hangs**: Check which recovery phase is failing
- **Service Assignment Failures**: Verify node capabilities and availability
- **Configuration Inconsistencies**: Ensure all nodes have the same configuration
- **Bootstrap Fails**: Check foreman health and storage worker availability
- **Config Corruption**: Verify BERT serialization/deserialization works

## Configuration Management

### Implementation Location
- `lib/bedrock/control_plane/config.ex`
- `lib/bedrock/control_plane/config/`

### Key Components

#### Cluster Configuration
```elixir
# lib/bedrock/control_plane/config.ex
# Main configuration structure

# lib/bedrock/control_plane/config/parameters.ex
# System parameters and tuning values

# lib/bedrock/control_plane/config/policies.ex
# Cluster policies and rules
```

#### Service Descriptors
```elixir
# lib/bedrock/control_plane/config/service_descriptor.ex
# Generic service description

# lib/bedrock/control_plane/config/log_descriptor.ex
# Log server configuration

# lib/bedrock/control_plane/config/resolver_descriptor.ex
# Resolver configuration

# lib/bedrock/control_plane/config/storage_team_descriptor.ex
# Storage server team configuration
```

#### Transaction System Layout
```elixir
# lib/bedrock/control_plane/config/transaction_system_layout.ex
# Defines how the transaction system is organized
```

### Development Patterns

#### Reading Configuration
```elixir
# Get current cluster configuration
{:ok, config} = YourCluster.fetch_config()

# Access specific configuration sections
config.parameters
config.policies
config.transaction_system_layout
```

#### Updating Configuration
```elixir
# Configuration updates should go through the Director
# This ensures consistency across the cluster
```

## Service Discovery and Management

### Foreman and Worker Pattern

Bedrock uses a Foreman/Worker pattern for service management:

```elixir
# lib/bedrock/service/foreman.ex
# Manages workers on a single node

# lib/bedrock/service/worker.ex
# Generic worker implementation

# lib/bedrock/service/worker_behaviour.ex
# Behavior that all workers must implement
```

### Development Patterns

#### Starting Services
```elixir
# Services are typically started by the Director during recovery
# Check the recovery implementation for service startup patterns
```

#### Monitoring Services
```elixir
# Check what services are running
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "bedrock"))

# Use Observer for visual monitoring
:observer.start()
```

## Testing Control Plane Components

### Unit Testing
- Test individual recovery phases in isolation
- Mock dependencies to test error conditions
- Use property-based testing for configuration validation

### Integration Testing
- Test full recovery process with multiple nodes
- Simulate node failures during recovery
- Test configuration changes across the cluster

### Common Test Patterns
```elixir
# Test recovery phase
test "determining durable version finds highest committed version" do
  # Setup mock log servers with different versions
  # Call the recovery phase
  # Assert correct version is selected
end

# Test node tracking
test "node tracking detects node failures" do
  # Start nodes
  # Simulate failure
  # Assert Director detects the failure
end
```

## Performance Considerations

### Recovery Performance
- Recovery time increases with cluster size
- Log replay can be time-consuming for large transaction logs
- Consider parallel recovery phases where possible

### Configuration Performance
- Configuration reads should be cached
- Configuration updates should be batched
- Use telemetry to monitor configuration access patterns

## Debugging Tips

### Recovery Issues
1. Check which recovery phase is failing
2. Look for network connectivity issues
3. Verify service dependencies are available
4. Check for resource constraints (memory, disk)

### Configuration Issues
1. Compare configuration across nodes
2. Check configuration file syntax
3. Verify configuration updates are propagated
4. Look for version mismatches

### Service Discovery Issues
1. Check Foreman logs for service startup failures
2. Verify worker implementations
3. Check for port conflicts
4. Monitor resource usage
