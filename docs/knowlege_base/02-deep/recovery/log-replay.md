# Log Replay Phase

**Copying committed transaction data from old logs to new log infrastructure.**

With new log services recruited and ready, recovery faces a critical data migration challenge: how do you transfer the current window of committed transactions from the old transaction system to the new one without losing data or breaking consistency? The `LogReplayPhase` solves this by copying transactions from the logs selected during planning to the newly recruited logs, ensuring that storage servers can continue processing from where they left off.

## Robustness Philosophy

Bedrock completely replaces logs with each recovery generation for robustness—the old logs could have bad disks, corruption, or other storage issues that only become apparent when reading them. Since recovery must read the transaction data anyway to verify what's recoverable, it writes that data to known-good storage in the new logs rather than trusting the old storage. 

The new transaction system layout will be completely different—different log services, node assignments, potentially different durability policies, and often a different number of logs total. This duplication ensures that all essential transaction data is stored on verified, reliable storage before the new system begins operation.

## Pairing Algorithm

The replay algorithm pairs each new log with source logs from the old system using round-robin assignment. If there are fewer old logs than new ones (common when scaling up), the algorithm cycles through the old logs repeatedly—for example, with 2 old logs and 4 new logs, the pairing would be: 
- new1 ← old1
- new2 ← old2 
- new3 ← old1 (cycles back)
- new4 ← old2

If no old logs are available (during initialization), new logs are paired with `:none` and remain empty until normal transaction processing begins. Each pairing operates efficiently in parallel, copying the established version range from its assigned old log to its target new log.

## Data Selection

Only committed transactions within the version vector are copied—uncommitted transactions are discarded since they were never guaranteed durable. This selective copying ensures that the new system begins operation with a clean, consistent transaction history that maintains all durability guarantees from the previous generation.

## Error Handling

If any log copying fails, the phase stalls with detailed failure information. However, if any log reports `:newer_epoch_exists`, the phase immediately returns an error since this director has been superseded and must halt all recovery attempts.

## Input Parameters

- `recovery_attempt.old_log_ids_to_copy` - Log IDs selected during log recovery planning phase
- `recovery_attempt.logs` - New log service assignments from recruitment 
- `recovery_attempt.version_vector` - Version range `{first_version, last_version}` for committed data
- `recovery_attempt.service_pids` - Process IDs for all locked log services

## Output Results

- Successful transition to data distribution phase with new logs populated with transaction data
- Or stall with `{:failed_to_copy_some_logs, failure_details}` for service issues
- Or immediate halt with `:newer_epoch_exists` for epoch conflicts

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/log_replay_phase.ex`

## Next Phase

Proceeds to [Sequencer Startup](09-sequencer-startup.md) to begin component initialization.