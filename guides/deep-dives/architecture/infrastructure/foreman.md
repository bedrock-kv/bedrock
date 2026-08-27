# Foreman

The Foreman manages worker processes and service lifecycle operations across cluster nodes. It provides centralized control over the creation, monitoring, and removal of storage and log workers that implement Bedrock's data plane services.

## Core Responsibilities

The Foreman implements several critical functions for service management:

### Worker Process Management

- Creates new log and materializer worker processes on demand
- Maintains registry of all running workers with health status
- Provides worker discovery and enumeration capabilities
- Coordinates worker shutdown and cleanup operations

### Service Lifecycle Operations

- Handles worker startup sequencing and dependency management
- Monitors worker health and reports status to the cluster
- Manages working directory creation and cleanup
- Coordinates service registration with cluster coordinators

### Resource Management

- Supervises worker processes through OTP supervision trees
- Manages working directories and persistent storage locations
- Provides batch operations for efficient worker management
- Implements graceful shutdown procedures with resource cleanup

## Embedded Distributed Architecture

The Foreman is a cornerstone of Bedrock's embedded distributed design, implementing the critical boundary between cluster-level orchestration and application-embedded data services. This component embodies how Bedrock moves beyond traditional client-server architectures to create data infrastructure that runs within application boundaries.

### Local-First Process Management

Unlike traditional distributed systems where data services run on dedicated infrastructure, the Foreman enables Bedrock to embed storage and log workers directly within application processes. This local-first approach means applications don't make network calls to remote databases—instead, they interact with co-located data services that the Foreman manages within the same compute environment.

The Foreman's worker management transforms how applications interact with persistent data. Rather than connecting to external database servers, applications work with local storage and log workers that provide the same transactional guarantees while eliminating network latency for many operations. This embedded approach enables new classes of applications that require both distributed consistency and local performance.

### Unified Failure Domains

The Foreman leverages one of the key advantages of embedded distributed systems: simplified failure scenarios. In traditional client-server architectures, applications must handle both application failures and database server failures as separate concerns. With the Foreman managing embedded workers, application failure and data service failure become the same event—dramatically simplifying failure handling and recovery logic.

When an application process fails, its embedded workers fail with it, creating a unified failure domain. The Foreman's supervision trees ensure that these failures are clean and predictable, while cluster-level recovery protocols handle redistributing responsibilities to healthy nodes. This unified approach eliminates the complex partial failure scenarios that plague client-server systems.

### Embedded Integration Advantages

The Foreman enables capabilities unique to embedded distributed architectures. By managing workers within application boundaries, it supports zero-copy data sharing between application logic and storage engines, dramatically improving performance for data-intensive operations. The close coupling also enables sophisticated features like application-aware caching, transaction scope optimization, and resource sharing between application code and data services.

This embedded approach also transforms deployment and operations. Applications and their data services deploy together as single units, eliminating version skew between application logic and database interfaces. The Foreman ensures that data services are always available when the application starts, removing the complex dependency management required in client-server architectures.

## Architecture Integration

The Foreman operates as a bridge between the control plane and data plane, translating high-level service requests into concrete worker processes.

## Worker Management Flow

The Foreman implements a structured workflow for worker lifecycle management:

1. **Creation Request**: Director or administrator requests new worker creation
2. **Resource Allocation**: Foreman allocates working directory and process resources
3. **Process Startup**: Worker process starts under supervision with proper configuration
4. **Health Verification**: Foreman waits for worker to report healthy status
5. **Service Registration**: Worker information is made available for cluster registration
6. **Ongoing Monitoring**: Continuous health monitoring with status reporting

This approach ensures workers are fully operational before being advertised to the cluster.

## Service Discovery Integration

Foremanprocesses coordinate with cluster service discovery:

### Service Information Provision

- Maintains authoritative list of running services on each node
- Provides compact service information for coordinator registration
- Maps internal worker processes to cluster-visible service identifiers
- Supports both individual and batch service registration operations

### Health Status Propagation

- Workers report health status to their local Foreman
- Foreman aggregates health information across all managed workers
- Provides cluster-wide health checking capabilities
- Enables automated recovery when workers become unhealthy

## Key Operations

### Worker Creation and Management

```elixir
# Create a new materializer worker with its shard assignment (stored in the
# worker's manifest and handed to the worker at startup)
{:ok, worker_ref} =
  Foreman.new_worker(foreman, "materializer_1", :materializer, params: %{"shard_id" => 1})

# Create a new log worker
{:ok, worker_ref} = Foreman.new_worker(foreman, "log_1", :log)

# List all running workers
{:ok, workers} = Foreman.all(foreman)

# List only materializer workers
{:ok, materializer_workers} = Foreman.materializer_workers(foreman)
```

### Health Monitoring

```elixir
# Workers report health to their foreman, naming their own process
:ok = Foreman.report_health(foreman, worker_id, {:ok, self()})
:ok = Foreman.report_health(foreman, worker_id, {:error, :unavailable})
```

### Service Registration Support

```elixir
# Get service information for coordinator registration
{:ok, services} = Foreman.get_all_running_services(foreman)

# Services returned as compact tuples
[
  {"materializer_1", :materializer, :bedrock_materializer_1},
  {"log_1", :log, :bedrock_log_1}
]
```

### Worker Cleanup Operations

```elixir
# Remove single worker with cleanup
:ok = Foreman.remove_worker(foreman, "materializer_1")

# Batch remove multiple workers
results = Foreman.remove_workers(foreman, ["materializer_1", "materializer_2", "log_1"])
# Returns: %{"materializer_1" => :ok, "materializer_2" => :ok, "log_1" => :ok}
```

## Working Directory Management

The Foreman manages persistent storage locations for workers:

### Directory Structure

- Each worker receives a dedicated working directory
- Directories persist worker configuration and data files
- Directory cleanup occurs during worker removal
- Failed cleanup operations are reported with detailed error information

### Cleanup Operations

- Worker termination triggers working directory cleanup
- Batch operations process multiple workers efficiently
- Error handling preserves system state on partial failures
- Directory removal failures include POSIX error codes and paths

## Health Monitoring System

Foreman implements comprehensive health monitoring:

### Worker Health States

- **`:ok`**: Worker is operational and ready for service
- **`{:failed_to_start, reason}`**: Worker failed during startup
- **`:starting`**: Worker is in startup phase
- **`:unknown`**: Health status is not yet determined

### Aggregated Health Checking

- Foreman folds its workers' health into a single verdict, recomputed on
  every change to the worker set: at spin-up, when a worker reports its
  own health, when one is created or removed, and when a monitored
  worker's process dies
- The Foreman monitors each running worker, so a process that dies stops
  counting toward healthy and stops appearing in the roll call the
  Foreman answers for service registration
- Two limits are worth knowing. The Coordinator withdraws a registration
  on deliberate removal rather than on death, so the cluster directory
  catches up at the next roll call. And the Foreman adopts a replacement
  the supervisor starts only when it registers within the recheck
  window; past that, the worker reads as stopped until the next spin-up
- Nothing reads the verdict yet. No call returns it and nothing emits it
  as telemetry, so an operator has no way to see it and cluster
  monitoring has nothing to act on

## Fault Tolerance Characteristics

The Foreman provides several layers of fault tolerance:

**Process Supervision**: Workers run under OTP supervision trees with automatic restart capabilities.

**Health Monitoring**: Continuous health checking enables rapid failure detection and response.

**Graceful Cleanup**: Worker removal includes proper resource cleanup and state management.

**Batch Operations**: Multiple worker operations are atomic, reducing partial failure scenarios.

**Error Propagation**: Detailed error information supports automated recovery decision-making.

## Performance Considerations

Foreman operations balance reliability with performance:

- **Supervision Overhead**: Worker supervision adds memory and CPU overhead but provides fault isolation
- **Health Check Frequency**: Monitoring intervals balance failure detection speed with system load
- **Batch Operations**: Multi-worker operations reduce message passing and improve efficiency
- **Working Directory I/O**: Directory operations can impact worker startup and shutdown performance
- **Service Registration Batching**: Compact service information reduces coordinator communication overhead

## Cross-Component Workflow Integration

### Service Registration Workflow Role

Foreman serves as the **service creation foundation** in Bedrock's service registration workflow:

**Foreman → Link → Coordinator → Director**

**Workflow Context**:

1. **Worker Process Creation**: Creates and manages storage and log worker processes on cluster nodes
2. **Service Advertisement**: Provides service information to Link for cluster-wide registration
3. **Health Monitoring**: Ensures worker processes remain operational and reports status
4. **Resource Management**: Manages working directories, supervision trees, and process lifecycle
5. **Capability Reporting**: Enables cluster-wide resource planning through service advertisements

**Key Handoff Points**:

- **To Link**: Provides service information for cluster registration
  - **Service Discovery**: `get_all_running_services/1` returns compact service tuples for registration
  - **Service Format**: Returns `{service_id, service_type, otp_name}` tuples for Coordinator registration
  - **Health Status**: Workers report health to Foreman before being advertised to cluster
  - **Batch Operations**: Efficient provision of multiple service advertisements simultaneously

- **From Director**: Receives worker creation requests during recovery
  - **Worker Specification**: `new_worker/4` creates specific worker types on demand, carrying startup params (a materializer's shard assignment) into the worker manifest
  - **Resource Allocation**: Coordinates working directory creation and process supervision
  - **Validation**: recovery validates a worker by locking it into the epoch and reading back its recovery info. Foreman health is a node-local summary, and plays no part in that decision
  - **Batch Creation**: Supports creation of multiple workers for efficient recovery scaling

**Error Propagation**:

- **Worker Creation Failures**: Creation errors → detailed error responses to Director → recovery planning adjustments
- **Health Monitoring Failures**: Worker health issues → status reporting through Link → potential cluster recovery triggers
- **Resource Cleanup Issues**: Working directory cleanup failures → detailed POSIX error reporting → administrative intervention

### Recovery Coordination Workflow Role

Foreman participates in the **infrastructure deployment** aspect of recovery:

**Director → Coordinator → Foreman → Transaction System Components**

**Workflow Context**:

1. **Infrastructure Provisioning**: Creates concrete worker processes from Director's abstract resource requirements
2. **Node-Local Management**: Handles all worker operations on individual cluster nodes
3. **Supervision Architecture**: Provides fault isolation through OTP supervision trees
4. **Resource Coordination**: Manages persistent storage locations and process resources

**Integration Details**:

- **Recovery Resource Planning**: Director uses Foreman capabilities for optimal worker placement
- **Fault Tolerance**: Process supervision ensures worker failures don't cascade to other components
- **State Management**: Working directory management provides persistent state for storage and log workers
- **Service Health**: Comprehensive health monitoring enables automated recovery decision-making

### Service Lifecycle Management Workflow

Foreman coordinates the **complete worker lifecycle**:

**Creation → Health Monitoring → Service Registration → Operational Management → Cleanup**

**Workflow Context**:

1. **Process Lifecycle**: Manages complete worker process lifecycle from creation to cleanup
2. **Health Coordination**: Continuous monitoring with status reporting to cluster components
3. **Resource Management**: Working directory allocation, supervision, and cleanup operations
4. **Service Integration**: Bridges local process management with distributed service discovery

**Integration Points**:

- **Embedded Architecture Support**: Enables application-embedded data services through local process management
- **Unified Failure Domains**: Process supervision creates clean failure boundaries for embedded distributed systems
- **Dynamic Scaling**: Supports runtime worker creation and removal for cluster capacity management
- **Operational Simplicity**: Single-node worker management eliminates complex distributed coordination

For detailed recovery process documentation, see **[Recovery Deep Dive](../../../deep-dives/recovery.md)**.

## See Also

- **[Director](../control-plane/director.md)**: Orchestrates worker creation through Foreman during recovery
- **[Link](link.md)**: Receives service advertisements from Foreman for cluster registration
- **[Coordinator](../control-plane/coordinator.md)**: Receives service information through Link from Foreman
- **[Storage](../data-plane/storage.md)**: Storage workers created and managed by Foreman
- **[Log](../data-plane/log.md)**: Log workers created and managed by Foreman
- **[Recovery](../../../deep-dives/recovery.md)**: Foreman role in cluster recovery infrastructure provisioning
