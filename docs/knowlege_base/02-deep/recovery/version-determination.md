# Version Determination Phase

**Establishing the highest transaction version guaranteed durable across the entire cluster.**

After planning service requirements, recovery faces a critical question: what's the highest transaction version that can be guaranteed durable across the entire cluster? The `VersionDeterminationPhase` answers this by examining storage teams to find the conservative baseline where all data is truly persistent.

## The Challenge

The challenge is that storage servers may be at different transaction versions due to processing lag, network delays, or partial failures. Recovery must find the highest version where enough replicas in every storage team have durably committed the data. This becomes the "recovery baseline"—all transactions at or below this version are guaranteed safe, while transactions above it may need to be replayed from logs.

## Algorithm Overview

The algorithm works by calculating a fault-tolerant version within each storage team, then taking the minimum across all teams. For each team, it sorts the durable versions of available replicas and selects the quorum-th highest version (where quorum = ⌊replication_factor/2⌋ + 1). This ensures that even if some replicas fail, the quorum can still provide the data.

### Example Calculation

If a team has three replicas at versions [95, 98, 100] and quorum is 2, the durable version is 98—the second-highest version that guarantees two replicas have the data even if the most current replica fails.

## Cluster-Wide Baseline

Once each team's durable version is calculated, the cluster-wide durable version becomes the minimum across all teams, ensuring that every piece of data is available at that version. This conservative approach guarantees that the recovered system can provide consistent access to all transactions at or below the recovery baseline.

## Team Health Classification

Teams are classified based on their replica availability:

- **Healthy**: Exactly the quorum number of replicas (optimal replication)
- **Degraded**: Fewer replicas (insufficient fault tolerance) or more replicas (resource inefficiency requiring rebalancing)
- **Insufficient**: Cannot meet quorum requirements

If any team lacks sufficient replicas to meet quorum, recovery stalls since the cluster cannot guarantee data durability.

## Fault Tolerance Implications

This version determination ensures that even accounting for potential replica failures during the new system's startup process, the cluster can still provide all data up to the established baseline. The conservative approach prioritizes data consistency and availability over maximizing the recovery baseline.

## Input Parameters

- `context.old_transaction_system_layout.storage_teams` - Storage team descriptors with replica assignments
- `recovery_attempt.storage_recovery_info_by_id` - Durable version information from each storage server
- `context.cluster_config.parameters.desired_replication_factor` - Used to calculate quorum requirements

## Output Results

- `recovery_attempt.durable_version` - Highest version guaranteed durable across all teams
- `recovery_attempt.degraded_teams` - List of team tags requiring rebalancing or repair

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/version_determination_phase.ex`

## Next Phase

Proceeds to [Log Recruitment](06-log-recruitment.md) to begin transforming vacancy placeholders into actual services.