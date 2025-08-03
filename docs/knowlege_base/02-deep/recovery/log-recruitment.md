# Log Recruitment Phase

**Transforming log vacancy placeholders into concrete service assignments.**

With vacancy placeholders defined, recovery now faces the practical challenge: how do you actually assign real services to fill those vacancies? The `LogRecruitmentPhase` transforms the abstract service plan into concrete assignments while navigating the constraints of what services are available and what data must be preserved.

## Balancing Efficiency with Safety

The challenge is balancing efficiency with safety. Recovery wants to reuse existing services to avoid the overhead of creating new ones, but it cannot touch services from the old transaction system since they contain committed transaction data that might be needed if this recovery fails and another one starts. This creates a careful selection process where recovery must find services that exist but weren't part of the previous configuration.

## Three-Phase Assignment Strategy

The algorithm follows a three-phase assignment strategy:

### Phase 1: Existing Service Reuse
First, it identifies existing log services that are available but weren't part of the old system—these are ideal candidates since they already exist and have no conflicting data.

### Phase 2: New Worker Creation  
If there aren't enough existing services to fill all vacancies, recovery creates new log workers, distributing them across available nodes using round-robin assignment to ensure fault tolerance. For example, if recovery needs 4 new workers across 3 nodes, it assigns workers to nodes in sequence: node1, node2, node3, node1.

### Phase 3: Service Locking
Finally, all recruited services—both existing and newly created—must be locked to establish exclusive control before proceeding.

## Critical Locking Step

The locking step is critical because recruited services might be handling other workloads or could be recruited by a competing recovery process. Only when all services are successfully locked can recovery be confident that the log layer is ready for transaction processing.

## Error Conditions

- If there aren't enough nodes to create needed workers, the phase stalls until conditions improve
- If any recruited service is already locked by a director with a newer epoch, recovery immediately halts with `:newer_epoch_exists`—this director has been superseded and must stop all recovery attempts

## Fault Tolerance Strategy

The round-robin distribution ensures fault tolerance by spreading critical components across different machines rather than concentrating them on adjacent nodes that might share failure modes. This geographical diversity improves the overall reliability of the log layer.

## Input Parameters

- `recovery_attempt.logs` - Map of vacancy placeholders to shard tags requiring log services
- `context.old_transaction_system_layout.logs` - Services to exclude from recruitment (preserve recovery data)  
- `context.available_services` - Currently discoverable log services from coordinator
- `context.node_capabilities.log` - Nodes capable of running new log workers

## Output Results

- `recovery_attempt.logs` - Vacancies replaced with actual recruited service IDs
- `recovery_attempt.transaction_services` - Service descriptors with lock status and PIDs
- `recovery_attempt.service_pids` - Direct PID mappings for recruited services

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/log_recruitment_phase.ex`

## Next Phase

Proceeds to [Storage Recruitment](07-storage-recruitment.md) to fill storage vacancy placeholders with actual storage services.