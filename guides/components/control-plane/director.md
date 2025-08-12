# Director

The Director orchestrates transaction system lifecycle and coordinates cluster recovery through epoch-based generation management. Created by the elected Coordinator leader, the Director serves as the singular authority for bringing up the data plane and maintaining the cluster in a writable state.

## Core Responsibilities

The Director implements centralized orchestration within Bedrock's control plane:

### Recovery Orchestration

- Coordinates systematic recovery of transaction system components
- Manages data plane startup sequencing and dependencies
- Handles component failure detection and replacement
- Orchestrates distributed consensus required for consistent operation

### Epoch-Based Generation Management

- Implements epoch counters to prevent split-brain scenarios during recovery
- Ensures only one Director instance operates per epoch
- Coordinates epoch transitions during leadership changes
- Manages service locking with generation precedence rules

### Service Lifecycle Management

- Requests worker creation from Foreman processes on cluster nodes
- Maintains registry of running services and their health status
- Coordinates service replacement when failures are detected
- Manages transaction system layout updates and propagation

## Architecture Integration

The Director operates as the central coordinator within Bedrock's control plane, serving as the primary orchestrator that receives service topology from the Coordinator and manages the complex recovery process across distributed components. The Director coordinates with Transaction Builders, Commit Proxies, Resolvers, and Sequencers in the control plane, while managing worker creation through Foreman processes in the data plane. The recovery process involves multiple phases including recovery planning, log recovery, storage recruitment, and version synchronization.

## Epoch Management System

Directors use epoch-based generation management to maintain consistency during concurrent recovery attempts:

### Generation Counter Semantics

- Epochs are monotonically increasing numbers managed by Coordinators
- Each Director instance receives a unique epoch at startup
- Services locked with newer epochs take precedence over older ones
- Processes from previous epochs terminate when detecting generation changes

### Split-Brain Prevention

- Only one Director can successfully lock services for a given epoch
- Concurrent Directors compete through epoch comparison
- Higher epochs win conflicts, forcing lower epoch Directors to terminate
- This eliminates complex coordination protocols while maintaining consistency

### Service Locking Protocol

```elixir
# Directors lock services with their epoch
lock_result = lock_service(service_id, epoch, director_id)

case lock_result do
  {:ok, lock_token} -> 
    # Director successfully locked service
    :proceed_with_recovery
  {:error, {:newer_epoch, current_epoch}} ->
    # Another Director with higher epoch exists
    :terminate_gracefully  
end
```

## Service Discovery Dependencies

The Director requires complete cluster topology to orchestrate recovery effectively:

### Coordinator-Provided Information

- **Service Directory**: Complete mapping of service IDs to node locations
- **Node Capabilities**: Capability advertisements from all cluster nodes  
- **Configuration State**: Current transaction system layouts and parameters

### Recovery Planning Requirements

- Directors analyze available nodes and capabilities
- Plans service placement based on fault tolerance requirements
- Creates new services when vacancies must be filled
- Coordinates with Foremanprocesses for worker creation

This dependency relationship ensures Directors have full knowledge of cluster resources before beginning recovery operations.

## Recovery Orchestration Flow

Directors manage a complex multi-phase recovery process:

1. **Service Discovery**: Receive populated service directory from Coordinator
2. **Recovery Planning**: Analyze current state and plan service placement
3. **Service Locking**: Lock required services with current epoch
4. **Component Startup**: Start transaction system components in dependency order
5. **Version Synchronization**: Coordinate MVCC version state across components
6. **Readiness Verification**: Confirm all components are operational
7. **Traffic Enablement**: Signal cluster is ready for transactions

Each phase includes rollback capabilities if failures occur, ensuring the cluster remains in a consistent state.

## Key Operations

### Transaction System Layout Management

```elixir
# Fetch current system layout for recovery planning
{:ok, layout} = Director.fetch_transaction_system_layout(director)

# Layout contains service assignments and configuration
%TransactionSystemLayout{
  sequencer: sequencer_service_id,
  commit_proxy: commit_proxy_service_id,
  resolver: resolver_service_id,
  logs: [log_service_ids],
  storage_teams: [storage_team_configs]
}
```

### Worker Creation Coordination

```elixir
# Request Foreman to create new worker on specific node
{:ok, worker_info} = Director.request_worker_creation(
  director,
  target_node,
  worker_id, 
  :storage,  # or :log
  timeout_ms
)

# Worker info contains service details for registration
%{
  id: "storage_5",
  otp_name: :bedrock_storage_5,
  kind: :storage,
  pid: worker_pid
}
```

### Service Discovery Notifications

```elixir
# Receive notification of new services from Coordinator
:ok = Director.notify_services_registered(director, service_infos)

# Receive notification of updated node capabilities  
:ok = Director.notify_capabilities_updated(director, node_capabilities)
```

### Health Monitoring

```elixir
# Send ping to Director with minimum read version
:ok = Director.send_ping(director, minimum_read_version)

# Director responds with pong if alive and version acceptable
:ok = Director.send_pong(director, from_node)
```

## Fault Tolerance Characteristics

The Director provides several fault tolerance mechanisms:

**Epoch-Based Consistency**: Generation management prevents split-brain scenarios and ensures only one Director operates per epoch.

**Service Discovery Resilience**: Directors receive complete topology from Coordinators, eliminating dependencies on partial information.

**Recovery Rollback**: Multi-phase recovery includes rollback capabilities when component startup fails.

**Health Monitoring**: Continuous health checks with automatic component replacement when failures are detected.

**Graceful Handoff**: Director transitions coordinate with Coordinator leader changes for seamless epoch management.

## Performance Considerations

Director operations balance recovery speed with consistency:

- **Centralized Coordination**: Single Director per epoch eliminates coordination overhead but creates bottleneck
- **Parallel Recovery**: Where possible, recovery phases execute in parallel to reduce startup time
- **Service Locality**: Worker creation considers node capabilities and network topology
- **Health Check Frequency**: Monitoring intervals balance failure detection speed with system overhead

## See Also

- [Coordinator](coordinator.md) - Creates and manages Director lifecycle
- [Recovery Deep Dive](../../deep-dives/recovery.md) - Detailed recovery process orchestration
- [Cluster Startup](../../deep-dives/cluster-startup.md) - Director role in cluster initialization
- [Transaction System Layout](../../quick-reads/transaction-system-layout.md) -  Service assignment coordination
