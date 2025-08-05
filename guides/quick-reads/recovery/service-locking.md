# Service Locking

**Establishing exclusive recovery authority to prevent split-brain scenarios.**

When multiple directors detect the same cluster failure simultaneously, they could attempt competing recovery processes that create conflicting system states. Service locking prevents this by establishing which [director](../../glossary.md#director) has exclusive authority to rebuild the system.

## How It Works

The locking phase secures control over services from the previous [transaction system layout](../../quick-reads/transaction-system-layout.md) using [epoch](../../glossary.md#epoch)-based precedence:

**Epoch Ordering**: Each recovery attempt carries a unique epoch identifier. Services accept locks only from the director with the highest epoch number, rejecting others with `:newer_epoch_exists` responses.

**Selective Targeting**: Only services containing persistent data require locking—specifically [log](../../components/data-plane/log.md) and [storage](../../components/data-plane/storage.md) components that must be protected during reconstruction.

**Parallel Operations**: Recovery attempts to lock all previous-generation services simultaneously, tolerating individual failures since cluster failures often involve partial component degradation.

## What Gets Locked

- **Log Services**: Contain committed transaction records that represent authoritative system history
- **Storage Services**: Contain user data and system state that would be expensive to recreate

Services not in the previous layout remain available for recruitment without protective locking.

## Critical Functions

1. **Halt Transaction Processing**: Locked services immediately stop accepting new operations to create a stable reconstruction foundation

2. **Collect State Information**: Locking returns essential recovery data from each service:
   - Process identifiers for direct communication
   - Transaction versions for consistency baselines
   - Component health status for reconstruction planning

3. **Prevent Split-Brain**: Exclusive control ensures only one director can rebuild the system, even during network partitions

## Recovery Path Decision

Locking results automatically determine the recovery path:

- **New Cluster**: No logs in previous layout → proceed to [initialization](path-determination.md)
- **Data Recovery**: Logs were defined → proceed to [data preservation phases](log-recovery-planning.md)

This guarantees the system never mistakes an existing deployment for a new one, protecting committed data.

## Error Handling

- **Recoverable**: Network timeouts and unreachable services don't halt recovery—the algorithm proceeds with available services
- **Fatal**: Any service reporting a newer epoch lock immediately terminates the current recovery attempt

---

**Next Phase**: [Path Determination](path-determination.md) — Classifying recovery scenarios based on locking results