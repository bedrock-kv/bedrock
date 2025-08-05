# Path Determination

**Recovery chooses between initializing a new cluster or preserving data from an existing system.**

After establishing exclusive control through [service locking](service-locking.md), recovery faces a critical decision: initialize a fresh cluster or recover existing data? This choice determines the entire recovery strategy.

## The Decision Logic

Recovery examines the previous [transaction system layout](transaction-system-layout.md) to make this determination:

- **No logs defined**: New cluster initialization 
- **Logs present**: Data recovery path (even if logs are currently unreachable)

This simple test provides foolproof protection against accidental data loss—recovery can never mistake an existing deployment for a new one.

## New Cluster Path

When no previous logs exist, recovery initializes a fresh cluster by creating:

1. **Storage Teams**: Two teams with distinct keyspace boundaries
   - User data team: handles application keys (empty string to `0xFF`)
   - System data team: handles system metadata (`0xFF` to end-of-keyspace)

2. **Vacancy Architecture**: Creates numbered placeholders for services
   - Log vacancies with initial version ranges `[0, 1]`
   - Storage vacancies distributed for desired replication factor

The initialization phase produces a complete architectural specification with:
- `durable_version`: 0 (fresh start)
- `version_vector`: {0, 0} (transaction ordering foundation)
- Complete vacancy mappings ready for service recruitment

This path is deterministic and always succeeds since it only creates in-memory data structures.

## Data Recovery Path

When logs existed previously, recovery must preserve valuable transaction data while rebuilding the system. This path requires three analytical phases:

1. **[Log Recovery Planning](log-recovery-planning.md)**: Determines what transaction data can be safely recovered from potentially compromised infrastructure

2. **[Vacancy Creation](vacancy-creation.md)**: Plans the new system architecture using vacancy abstractions

3. **[Version Determination](version-determination.md)**: Establishes the highest transaction version guaranteed durable across the cluster

## Next Steps

**New Cluster**: Proceeds to [Vacancy Creation](vacancy-creation.md) for infrastructure recruitment

**Data Recovery**: Continues to [Log Recovery Planning](log-recovery-planning.md) for data preservation analysis

Both paths eventually converge on service recruitment, but data recovery requires significantly more analysis to ensure transaction data integrity.

## Implementation

- Path selection: `lib/bedrock/control_plane/director/recovery.ex`
- Initialization: `lib/bedrock/control_plane/director/recovery/initialization_phase.ex`