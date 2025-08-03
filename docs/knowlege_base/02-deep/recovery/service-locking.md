# Service Locking Phase

**Establishing exclusive control over the remnants of the failed system.**

Recovery establishes exclusive control by selectively locking services from the old transaction system layout. Only services that were part of the previous system are candidates for locking—these contain data that must be preserved during recovery. Other available services remain unlocked until recruitment phases assign them to specific roles.

## Purpose and Rationale

Service locking serves three critical purposes:

1. **Prevent [split-brain](../glossary.md#split-brain)** - Each service accepts locks from only one director at a time, with epoch-based precedence ensuring exclusive control
2. **Halt transaction processing** - Locked services immediately stop all normal operations (logs stop accepting transactions, storage stops pulling from logs) 
3. **Validate reachability** - The lock operation confirms services are genuinely operational and returns actual process PIDs plus recovery state information (oldest/newest transaction versions, durability status), critical in dynamic environments where machines restart with new addresses

## Algorithm Details

Locking attempts all services in parallel. Individual service failures (unreachable, timeout) are ignored since the system may have failed due to partial component failures. Recovery gathers and locks as many services as possible from the old system, determining in subsequent phases what can be salvaged. 

However, if any service is already locked with a newer epoch, it rejects the request with `:newer_epoch_exists`, indicating this director has been superseded and must halt recovery entirely. Unlike other failures that cause recovery to stall and retry, epoch conflicts are fatal errors that stop all further recovery attempts. This strict epoch ordering prevents conflicts between concurrent directors.

## Path Determination

The recovery path is determined by whether logs existed in the previous generation:
- **No logs in previous layout**: No transaction system existed → initialization required  
- **Logs in previous layout**: Previous system with data → recovery from existing state (regardless of how many services are successfully locked)

This automatic path selection eliminates the need for separate cluster state detection mechanisms. Critically, if logs were defined but none can be locked due to failures, recovery proceeds to data recovery phases rather than initialization - preventing accidental data loss by ensuring existing systems are never mistaken for new deployments.

## Input Parameters

- `context.old_transaction_system_layout` - Previous system configuration with logs and storage teams
- `context.available_services` - Currently discoverable services from coordinator
- `recovery_attempt.epoch` - Current recovery epoch for locking precedence  

## Output Results

- `locked_service_ids` - Set of successfully locked service identifiers
- `log_recovery_info_by_id` - Recovery state information for each locked log service
- `storage_recovery_info_by_id` - Recovery state information for each locked storage service  
- `transaction_services` - Service descriptors with PIDs and status for locked services
- `service_pids` - Direct PID mappings for locked services

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/locking_phase.ex`

## Next Phase

Recovery proceeds to [Path Determination](02-path-determination.md) to decide between new cluster initialization or data recovery based on locking results.