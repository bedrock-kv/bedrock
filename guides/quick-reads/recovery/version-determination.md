# Version Determination

**Finding the highest transaction version guaranteed durable across the cluster.**

Version determination solves a critical distributed systems challenge: identifying the maximum [version](../../glossary.md#version) safely recoverable when [storage teams](../../glossary.md#storage-team) report different durability states. This establishes the recovery baseline—the foundation for all subsequent phases.

## The Challenge

Storage servers inevitably operate at different versions due to network delays and processing variations. Recovery cannot simply use the highest reported version, as that replica might fail during reconstruction. Instead, it must find a fault-tolerant baseline where sufficient replicas guarantee data availability.

## Mathematical Approach

The algorithm uses two-tier analysis:

### Storage Team Analysis

For each team, recovery applies quorum mathematics to replica versions. In a three-replica team reporting versions [95, 98, 100], the durable version becomes 98—ensuring two replicas have the data even if the highest-version replica fails.

### Cluster-Wide Baseline

The cluster baseline takes the minimum across all team baselines, ensuring every data piece remains available throughout the cluster.

## Team Health Classification

Teams are classified during analysis:

- **Healthy**: Exact replica count, optimal operation
- **Degraded**: Over/under-replicated, needs rebalancing  
- **Insufficient**: Below quorum threshold, causes recovery termination

## Guarantees

The conservative approach prioritizes availability over maximizing recoverable versions. The established baseline guarantees data access even with additional failures during reconstruction.

## Integration

Version determination outputs enable subsequent phases:

- Baseline informs [log replay](log-replay.md) boundaries
- Team health guides [storage recruitment](storage-recruitment.md) planning
- Durable version sets [sequencer startup](sequencer-startup.md) baseline

## Implementation

**Input**: Storage team layouts, recovery info, replication configuration  
**Output**: Durable version baseline, degraded team list  
**Source**: `lib/bedrock/control_plane/director/recovery/version_determination_phase.ex`

---

**Next**: [Log Recruitment](log-recruitment.md) - Replacing vacancy placeholders with operational log services
