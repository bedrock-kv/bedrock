# Log Recruitment

**Building reliable log infrastructure by replacing potentially compromised services with verified ones.**

During recovery, Bedrock must fill [vacancy placeholders](vacancy-creation.md) in the new system architecture with actual [log services](../../deep-dives/architecture/data-plane/log.md). Unlike other recovery phases that attempt to preserve existing components, log recruitment takes an aggressive replacement approach: prioritize reliability over efficiency by using fresh services instead of salvaging potentially compromised ones.

This "clean slate" strategy works because logs contain no unique persistent state—every committed transaction exists identically across all logs in the system. The old logs contain transaction history that will be copied to newly recruited logs during the subsequent [log replay](log-replay.md) phase, but their infrastructure cannot be trusted for continued operational use.

## Three-Tier Recruitment Strategy

Log recruitment employs a systematic approach to find and assign reliable services:

**Tier 1: Service Discovery** - First, identify existing log services in the cluster that were *not* part of the failed system. These services are operational and uncompromised, making them ideal candidates for immediate recruitment.

**Tier 2: New Service Creation** - When existing services are insufficient, create fresh log workers using round-robin distribution across cluster nodes. This geographic diversity ensures that multiple node failures cannot simultaneously compromise all log infrastructure serving any particular [shard](../../glossary.md#shard).

**Tier 3: Service Locking** - Lock all recruited services (both existing and newly created) to establish exclusive control, validate operational readiness, and prevent split-brain scenarios where multiple recovery processes compete for the same services.

## Critical Error Conditions

Log recruitment implements fail-fast error handling for scenarios that compromise recovery safety:

- **Insufficient Node Capacity**: When the cluster lacks enough nodes with log worker capabilities to create required services, recovery cannot proceed safely
- **Newer Epoch Detection**: If any service reports being locked by a higher epoch number, this recovery attempt has been superseded and must terminate
- **Service Unresponsiveness**: Services that fail to respond to locking requests indicate broader infrastructure problems and trigger replacement rather than repair

## Data Flow

The recruitment process transforms abstract vacancy placeholders into concrete operational infrastructure:

- **Input**: `recovery_attempt.logs` containing vacancy placeholders like `{:vacancy, 1}` mapped to shard combinations
- **Processing**: Systematic assignment of discovered or newly created services according to the three-tier strategy
- **Output**: Concrete service identifiers replacing all placeholders, plus service descriptor mappings and process relationships for coordination

This establishes the trustworthy log foundation needed for subsequent [storage recruitment](storage-recruitment.md) and [log replay](log-replay.md) phases.

---

**Implementation**: `lib/bedrock/control_plane/director/recovery/log_recruitment_phase.ex`

**See Also**: [Recovery Overview](../recovery.md) | [Log Replay](log-replay.md) | [Storage Recruitment](storage-recruitment.md)
