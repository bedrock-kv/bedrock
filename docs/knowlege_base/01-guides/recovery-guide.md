# Recovery Guide

**Understanding Bedrock's distributed system recovery process.**

Recovery orchestrates the complex coordination required to bring a distributed database cluster from a failed or starting state to full operation. This guide explains how recovery works, when it triggers, and how its phases rebuild the transaction system.

## Recovery Philosophy

Bedrock implements a "fail-fast" recovery philosophy inspired by both Erlang/OTP and FoundationDB. Instead of complex health checking and partial failure handling, the system monitors critical components with `Process.monitor/1` and triggers immediate, complete recovery when any transaction component fails.

This approach trades complexity for reliability: rather than attempting to patch around failures, the system rebuilds the entire transaction layer from a known-good state. Recovery typically completes in seconds, making the trade-off practical for distributed database workloads that require strict consistency.

Recovery uses epoch-based generation management to prevent split-brain scenarios. Each recovery increments the cluster epoch, serving as a generation counter that ensures clean transitions between system configurations. Processes from previous epochs detect newer generations and terminate themselves.

## When Recovery Triggers

Recovery begins when any component critical to transaction processing fails:

**Critical Components**: Coordinator, Director, Sequencer, Commit Proxies, Resolvers, Transaction Logs  
**Non-Critical Components**: Storage Servers, Gateways, Rate Keeper

The distinction reflects architectural responsibilities. Transaction components participate in every commit and must maintain strict consistency. Storage and gateway failures are handled locally through data distribution and client retry mechanisms.

Component failures are detected through different mechanisms:
- **Coordinator Failure**: Raft heartbeat timeout triggers leader election
- **Director Failure**: `Process.monitor/1` in Coordinator triggers restart with incremented epoch  
- **Transaction Component Failure**: Director monitors all components; any failure triggers Director exit

## Recovery Prerequisites

Recovery begins only after the cluster fully forms and establishes stable coordination. The Coordinator must populate its service directory through Raft consensus before starting a Director. This prerequisite story—including node startup, leader elections, service registration, and the critical timing coordination—is covered in detail in [Cluster Startup](cluster-startup.md).

Once cluster formation completes, the Coordinator starts a Director with complete knowledge of available resources, and recovery begins.

## Recovery Process

### Phase 1: Foundation - Startup and Service Locking

Recovery begins when the Coordinator starts a Director with the populated service directory. The Director records the recovery start time and immediately attempts to lock services from the previous transaction system layout.

Service locking establishes exclusive Director control and prevents split-brain scenarios where multiple directors attempt concurrent control. Services are locked with the current epoch; services with older epochs detect the newer epoch and terminate.

**The Branch Point**: What gets locked determines the recovery path:
- **Nothing locked** → First-time initialization (new cluster)
- **Services locked** → Recovery from existing persistent state

### Phase 2: Initialization or Discovery

**New Cluster Path**: Creates initial transaction system layout with log descriptors covering evenly distributed key ranges and storage teams with configured replication factor.

**Existing Cluster Path**: Discovers logs requiring recovery from the previous system layout, then determines the highest committed version across all logs to establish the consistent recovery point.

### Phase 3: Resource Allocation - Vacancies and Recruitment

Recovery creates vacancy placeholders for missing services, then fills them through recruitment:

**Log Recruitment**: Assigns or creates log workers to handle transaction durability  
**Storage Recruitment**: Assigns or creates storage workers for data storage and replication

Recruitment prioritizes existing services over creating new ones, preserving data locality and reducing recovery time.

### Phase 4: Data Consistency - Replay and Distribution

**Log Replay**: Replays transactions from old logs to ensure all committed transactions appear in the new system. This phase handles the critical data migration between transaction system generations.

**Data Distribution**: Fixes data distribution across storage servers to ensure proper sharding and replication according to the new layout.

### Phase 5: Transaction System Startup

Recovery starts transaction processing components in dependency order:

1. **Sequencer**: Provides version assignment for MVCC consistency
2. **Commit Proxies**: Batch transactions and coordinate commits
3. **Resolvers**: Implement conflict detection for concurrent transactions

Each component startup validates that the previous components are operational before proceeding.

### Phase 6: Validation and Layout Construction

**Validation**: Performs comprehensive checks ensuring all components are properly configured and ready for transaction processing.

**Transaction System Layout**: Constructs the complete system topology containing sequencer, commit proxies, resolvers, logs, storage teams, and service descriptors. This becomes the blueprint defining how the distributed system operates.

### Phase 7: Persistence and Monitoring

**Persistence**: Constructs a system transaction containing the full cluster configuration and submits it through the entire data plane pipeline. This simultaneously persists the new configuration and validates that all transaction components work correctly.

**Monitoring**: Sets up monitoring of all transaction system components using `Process.monitor/1`. Any subsequent failure triggers immediate director shutdown and recovery restart, implementing the fail-fast philosophy.

### Phase 8: Cleanup and Completion

**Cleanup**: Removes unused workers no longer needed in the new system layout.

**Recovery Complete**: System transitions to operational mode, ready to process client transactions.

## Recovery Patterns

### Stalled Recovery and Resource Addition

When recovery stalls due to insufficient resources, it enters a waiting state rather than failing permanently. New nodes joining with required services trigger Director notification to retry recovery with the expanded resource set. This only occurs when recovery is already blocked - functioning systems continue normally when nodes join.

### Durable Service Integration

Logs and storage servers persist data across restarts and epoch changes. When nodes boot, they start locally available durable services, which then advertise to the cluster through the Gateway-Coordinator service registration flow. The Director incorporates these services through standard recruitment phases.

### Leader Changes During Recovery

Leader election interrupts recovery as the old Director terminates and a new Coordinator starts a fresh Director with incremented epoch. The new leader inherits the service directory through Raft state, ensuring service continuity while resetting recovery to a known-good starting point.

## See Also

- [Cluster Startup](cluster-startup.md) - How clusters form before recovery begins
- `Bedrock.ControlPlane.Director.Recovery` - Main recovery orchestrator
- `Bedrock.ControlPlane.Director.Recovery.*Phase` - Individual phase implementations
- `Bedrock.ControlPlane.Config.RecoveryAttempt` - Recovery state management