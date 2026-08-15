# Log Recovery Planning

**Determining what committed transaction data can be safely recovered from existing logs.**

Log recovery planning analyzes surviving logs to identify which [transactions](../transactions.md) can be safely restored. This phase only runs for existing clusters with potentially valuable data—new clusters skip directly to clean infrastructure creation.

## Core Strategy

Bedrock stores every committed transaction on every [log](../../deep-dives/architecture/data-plane/log.md), creating natural redundancy. During recovery, some logs may be unavailable or contain different data ranges due to failures.

The planning algorithm:

1. **Groups logs by shard** - Logs serving the same key ranges form analysis groups
2. **Checks quorum requirements** - Each shard needs sufficient logs for data validation  
3. **Finds common version range** - Identifies the latest version safely recoverable across all shards
4. **Selects optimal logs** - Chooses log combinations maximizing recoverable data

## Shard Analysis

For each shard, recovery examines all possible combinations of available logs meeting quorum requirements:

```text
Shard Alpha: Logs 1, 2, 3 (requires 2 for quorum)
- Logs 1 & 2 available with data through version 100
- Log 3 unreachable
- Result: Can recover through version 100 ✓
```

## Cross-Shard Consensus

Individual shard analysis isn't enough. Recovery uses the **minimum version across all shards** as the global recovery baseline:

```text
Shard Alpha: Max recoverable version 100
Shard Beta:  Max recoverable version 85  
Shard Gamma: Max recoverable version 95
Global recovery baseline: 85 (minimum)
```

This conservative approach ensures every recovered transaction achieved full cross-shard replication.

## Data Maximization

Recovery attempts to copy from all available logs within each shard, not just the minimum required. This provides more transaction history for [storage servers](../../deep-dives/architecture/data-plane/storage.md) that need to catch up during their own recovery.

## Failure Handling

If any shard cannot meet quorum requirements, recovery fails with `:unable_to_meet_log_quorum`. This prevents recovery from proceeding with insufficient data validation guarantees.

## Algorithm Inputs and Outputs

**Inputs:**

- Previous system configuration (log assignments and ranges)
- Current log status (availability and version ranges)  
- Target system requirements (quorum parameters)

**Outputs:**

- `old_log_ids_to_copy` - Which logs to preserve data from
- `version_vector` - Transaction range bounds for recovery

## Next Phase

Success leads to [vacancy creation](vacancy-creation.md), which uses the established version vector to plan the new system architecture.

**Implementation**: `lib/bedrock/control_plane/director/recovery/log_recovery_planning_phase.ex`
