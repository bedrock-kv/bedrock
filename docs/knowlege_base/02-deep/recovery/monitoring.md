# Monitoring Phase

**Establishing operational monitoring and marking recovery as complete.**

The `MonitoringPhase` sets up monitoring of all transaction system components and marks recovery as complete. It establishes process monitoring for sequencer, commit proxies, resolvers, logs, and storage servers. Any failure of these critical components will trigger immediate director shutdown and recovery restart.

## Fail-Fast Philosophy

This monitoring implements Bedrock's fail-fast philosophy—rather than attempting complex error recovery, component failures cause the director to exit and let the coordinator restart recovery with a new epoch. The monitoring setup represents the final step before the cluster becomes operational.

The fail-fast approach maintains system reliability by ensuring that any component failure triggers a complete recovery rather than attempting partial repairs that might introduce subtle inconsistencies or corrupt system state.

## Component Coverage

The monitoring covers all transaction processing components that require strict consistency:

- **Sequencer**: Global version number authority - failure requires immediate recovery
- **Commit Proxies**: Transaction coordination components - failure affects transaction processing  
- **Resolvers**: MVCC conflict detection - failure compromises transaction isolation
- **Logs**: Transaction durability - failure affects data persistence
- **Director**: Recovery and coordination management - failure triggers coordinator takeover

## Storage Server Exception

Storage servers are notably excluded from monitoring. They handle failures independently through data distribution and repair mechanisms. The transaction processing components, however, must maintain strict consistency, so any failure requires complete recovery.

Storage servers can experience individual failures, lag behind in processing, or temporarily become unavailable without compromising overall system consistency. Their failure handling is managed through:

- Data replication across multiple storage servers
- Automatic failover to healthy replicas  
- Independent recovery and catch-up mechanisms
- Load balancing and redistribution

## Recovery Completion

Once monitoring is active, the director shifts from recovery mode to operational mode. The recovery attempt is marked as complete, and the cluster begins accepting normal transaction processing requests. This transition represents the successful completion of the entire recovery journey from failure state to operational distributed database.

## System Readiness

At this point, the system has achieved:

- All components operational and monitored
- Complete system configuration durably persisted
- Transaction processing pipeline validated
- Monitoring established for continued operation
- Recovery state cleaned up and marked complete

## Input Parameters

- Complete transaction system layout with all component PIDs
- Validated operational status of all transaction components

## Output Results

- Process monitoring established for all critical components
- Recovery marked as complete  
- System transitioned to operational mode
- Director ready to handle normal transaction processing

## Rationale

Fail-fast monitoring for transaction components maintains consistency while excluding storage components that handle failures independently. This approach ensures that any transaction processing issues trigger comprehensive recovery rather than allowing the system to operate in a potentially inconsistent state.

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/monitoring_phase.ex`

## Recovery Complete

This marks the end of the recovery process. The system has been transformed from a potentially chaotic failure state into a fully operational, consistent, and monitored distributed database ready to serve transaction processing requests.

---

**See Also:**
- [Recovery Overview](README.md) - Complete recovery system documentation
- [Recovery Guide](../knowlege_base/01-guides/recovery-guide.md) - Operational recovery guidance
- [Bedrock Architecture](../bedrock-architecture.md) - Overall system architecture