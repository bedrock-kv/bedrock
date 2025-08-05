# Storage Recruitment

**Assigns storage services to vacancies while preserving existing data.**

Storage recruitment fills [vacancy placeholders](vacancy-creation.md) with concrete storage services, following strict data preservation rules that protect committed state from the previous system.

## Core Principle: Data Safety First

Unlike [log recruitment](log-recruitment.md), storage recruitment must handle services containing irreplaceable committed data—user records, computed indexes, and historical state that cannot be recreated. The recruitment algorithm prioritizes data preservation over efficiency.

## Three-Tier Assignment Strategy

**Absolute Preservation**: All storage services from the previous [transaction system layout](transaction-system-layout.md) are excluded from recruitment. These contain valuable data and remain untouched for future access.

**Conservative Reuse**: Available storage services not part of the previous system become candidates for filling vacancies. Their state is treated with caution.

**Controlled Creation**: When existing services cannot satisfy requirements, new storage workers are created using fault-tolerant round-robin placement across available nodes.

## Recruitment Process

1. **Vacancy Assessment**: Catalog all storage vacancy placeholders requiring concrete service assignments
2. **Candidate Pool Analysis**: Identify available services, excluding previous-system services and already-assigned services
3. **Assignment Strategy**: Fill vacancies with candidates, creating new workers if needed
4. **Service Locking**: All recruited services acquire [service locks](service-locking.md) for exclusive control

## Error Handling

**Resource Insufficiency**: Recruitment stalls if insufficient nodes exist for fault-tolerant worker creation.

**Locking Failures**: If any service fails to acquire its lock, the entire phase stalls to prevent partial control scenarios.

**Epoch Supersession**: If a recruited service reports a newer epoch, recovery terminates immediately with `:newer_epoch_exists`.

## Next Phase

Upon completion, all storage vacancies are filled with locked services. Recovery proceeds to [log replay](log-replay.md) to copy committed transaction data from the previous system.

---

**Implementation**: `lib/bedrock/control_plane/director/recovery/storage_recruitment_phase.ex`
