# Storage Recruitment Phase

**Ultra-conservative assignment of storage services that prioritizes data preservation.**

With storage team vacancies defined, recovery faces a more complex assignment challenge than log recruitment: how do you fill positions that require persistent data without accidentally destroying valuable information? The `StorageRecruitmentPhase` solves this challenge by implementing an ultra-conservative approach that prioritizes data preservation above all else—storage services contain persistent data that's expensive to recreate and potentially irreplaceable if lost.

## The Data Stakes

The fundamental difference from log recruitment lies in the data stakes. Logs contain transaction records that can be replayed from other logs, but storage services contain the actual committed data—user records, system state, and historical information that took time and computation to build. This means recovery must be extremely careful about which storage services it touches and how it handles them.

## Strict Preservation Hierarchy

The algorithm follows a strict preservation hierarchy:

1. **Preserve all services from the old system** - Never touch them during recruitment
2. **Reuse available services** - That weren't part of the old system  
3. **Create new workers** - Only as a last resort

This hierarchy ensures that valuable persistent data is never accidentally destroyed during the recovery process.

## Assignment Process

### Vacancy Counting
The assignment process starts by counting all storage vacancies across all teams—these `{:vacancy, n}` placeholders created during vacancy creation represent the gaps between current replica counts and desired replication factors.

### Candidate Identification
Recovery then identifies candidate services: available storage services that aren't part of the old transaction system and aren't already assigned to non-vacancy positions in the new system.

### Direct Assignment or Worker Creation
If there are enough candidates to fill all vacancies, recovery assigns them directly. If not, it calculates how many new workers are needed and creates them using round-robin distribution across available nodes for fault tolerance. For example, if recovery needs 5 storage workers across 3 nodes, it assigns them to nodes in sequence: node1, node2, node3, node1, node2.

### Comprehensive Locking
Finally, all recruited services—both existing and newly created—must be successfully locked before recovery can proceed. This establishes exclusive control and prevents interference from other processes or competing directors.

## Error Conditions

- The phase stalls if there aren't enough nodes to create needed workers
- The phase stalls if any recruited service fails to lock  
- If any recruited service is already locked by a director with a newer epoch, recovery immediately halts with `:newer_epoch_exists`—this director has been superseded and must stop all recovery attempts

## Data Safety Guarantees

This ultra-conservative approach provides several data safety guarantees:

- Existing storage data is never destroyed accidentally
- Only clean or unused storage services become candidates for recruitment
- Multiple layers of validation prevent data corruption
- Persistent data remains available for future recovery attempts if needed

## Input Parameters

- `recovery_attempt.storage_teams` - Storage team descriptors with vacancy placeholders requiring services
- `context.old_transaction_system_layout.storage_teams` - Services to exclude from recruitment (preserve data)
- `context.available_services` - Currently discoverable storage services from coordinator  
- `context.node_capabilities.storage` - Nodes capable of running new storage workers

## Output Results

- `recovery_attempt.storage_teams` - Vacancies replaced with actual recruited service IDs
- `recovery_attempt.transaction_services` - Service descriptors with lock status and PIDs
- `recovery_attempt.service_pids` - Direct PID mappings for recruited services

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/storage_recruitment_phase.ex`

## Next Phase

Proceeds to [Log Replay](08-log-replay.md) to copy committed transaction data to the new log infrastructure.