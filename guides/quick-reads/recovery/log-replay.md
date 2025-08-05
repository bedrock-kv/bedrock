# Log Replay: Data Migration to Trusted Infrastructure

**Copying committed transactions from potentially compromised logs to verified new infrastructure.**

The log replay phase transfers all committed transaction data from old [logs](../../components/data-plane/log.md) to newly recruited log services. Rather than attempting to salvage potentially compromised infrastructure, Bedrock systematically copies transaction data to verified, reliable storage before the new system begins operation.

## Core Process

**Smart Pairing Algorithm**  
Recovery pairs each new log with old logs using round-robin distribution. When scaling from 2 to 4 logs:
- New log 1 ← Old log 1  
- New log 2 ← Old log 2
- New log 3 ← Old log 1 (cycles back)
- New log 4 ← Old log 2

**Selective Data Migration**  
Only committed transactions within the established [version](../../glossary.md#version) range determined during [version determination](version-determination.md) undergo copying. Uncommitted transactions are deliberately excluded to maintain system consistency.

**Parallel Execution**  
Each old-to-new log pairing operates in parallel, maximizing throughput while maintaining strict consistency requirements.

## Reliability Philosophy

Bedrock chooses data integrity over operational efficiency. Even though old logs contain correct transaction data, recovery copies this information to newly recruited logs rather than reusing potentially compromised storage. This ensures all transaction data resides on verified infrastructure before system startup.

The new [transaction system layout](transaction-system-layout.md) often differs significantly—different log services, node assignments, and potentially different durability policies. Complete data migration ensures compatibility between system generations.

## Error Handling

- **Service Failures**: Controlled stall with detailed diagnostics
- **Epoch Conflicts**: Immediate termination if `:newer_epoch_exists` indicates a newer recovery attempt
- **Fail-Fast**: Clean termination enables coordinator restart with fresh state

## Phase Integration

**Prerequisites**: [Log recruitment](log-recruitment.md), [version determination](version-determination.md), [service locking](service-locking.md)  
**Next Phase**: [Sequencer startup](sequencer-startup.md) with populated transaction data  
**Implementation**: `lib/bedrock/control_plane/director/recovery/log_replay_phase.ex`

This phase marks the transformation from architectural planning to operational infrastructure capable of serving transaction workloads with complete data integrity confidence.