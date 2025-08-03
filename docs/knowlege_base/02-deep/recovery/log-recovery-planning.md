# Log Recovery Planning Phase

**Determining what committed transaction data can be safely recovered from potentially failed logs.**

When logs existed in the previous layout, the `LogRecoveryPlanningPhase` determines what data can be safely recovered from potentially failed or unavailable logs. During normal operation, Bedrock sends _every_ transaction to _all_ logs in the system to maintain global ordering—every log contains a record of every committed transaction (see [Transaction Deep Dive](../knowlege_base/02-deep/transactions-deep.md#step-46-durable-log-persistence)). However, log failures may be the reason recovery is necessary, so not all logs can be guaranteed available or intact.

## Algorithm Overview

The algorithm groups available logs by their range tags (shards) and evaluates what data can be safely recovered from each shard. Within each shard, it generates all possible log combinations meeting quorum requirements, calculates recoverable version ranges for each combination, and selects the combination providing the largest data range. 

The final recovery decision takes the cross-shard minimum of these ranges—only including the highest transaction version guaranteed complete across every participating shard. By maximizing the transaction history copied to the new generation, we improve the recovered system's tolerance for storage server lag.

## Quorum Requirements

Recovery requires sufficient logs from each shard to guarantee data consistency. If any shard cannot meet its quorum requirements, the phase stalls with `:unable_to_meet_log_quorum`.

The cross-shard approach ensures that only transaction versions that achieved sufficient replication across all shards are included in the recovery. This conservative approach maintains strict consistency guarantees while maximizing the amount of preservable transaction history.

## Architectural Differences from FoundationDB

This shard-aware recovery approach differs architecturally from FoundationDB's method described in their technical paper. FoundationDB uses Known Committed Version (KCV) tracking by commit proxies, where recovery calculates the minimum of Durable Versions (DV) from logs but constrains the recoverable range by what proxies have confirmed as committed through KCV advancement. 

This creates a "version lag" issue: if a transaction successfully persists to all logs but no subsequent transaction occurs to advance the KCV, that transaction is discarded during recovery despite being fully durable. Bedrock directly examines log persistence across shards without proxy-dependency, recovering any transaction that achieved cross-shard quorum persistence regardless of subsequent proxy activity, thus avoiding the version lag limitation while ensuring stronger consistency guarantees.

## Storage Server Benefits

By copying from all available logs instead of a minimal subset, recovery preserves more historical transaction data in the new log generation. This extended history provides storage servers with more opportunities to find their current position and reduces the risk that lagging storage servers will fall behind the available log data during their own recovery process.

Storage servers pull transaction data from logs to maintain their local state. When storage servers are behind the recovered log baseline, they must process more transactions to catch up. The extended history provides a larger window for storage servers to synchronize without losing access to required transaction data.

## Input Parameters

- `context.old_transaction_system_layout.logs` - Previous log layout with log descriptors
- `recovery_attempt.log_recovery_info_by_id` - Recovery information from locked log services
- `context.cluster_config.parameters.desired_logs` - Used to calculate quorum requirements

## Output Results

- `old_log_ids_to_copy` - List of log IDs selected for data preservation
- `version_vector` - Consistent version bounds {oldest_version, newest_version} for the selected logs

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/log_recovery_planning_phase.ex`

## Next Phase

Proceeds to [Vacancy Creation](04-vacancy-creation.md) to plan the new system architecture.