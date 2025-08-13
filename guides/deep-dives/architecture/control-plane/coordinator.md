# Coordinator

The [Coordinator](../../glossary.md#coordinator) maintains cluster state through [Raft](../../glossary.md#raft) distributed [consensus](../../glossary.md#consensus) and orchestrates [Director](../../glossary.md#director) lifecycle during leadership changes. It serves as the authoritative source for cluster configuration and service directory information, enabling distributed coordination across all nodes.

**Location**: [`lib/bedrock/control_plane/coordinator.ex`](../../../lib/bedrock/control_plane/coordinator.ex)

## Consensus Leadership

Coordinators use Raft consensus to maintain cluster configuration and service directories. The leader Coordinator persists changes through consensus, ensuring consistent state replication across all coordinator nodes and automatic failover during leader elections.

Leader readiness states prevent race conditions: `:leader_waiting_consensus` delays [Director](../../glossary.md#director) startup until service directory population completes, then transitions to `:leader_ready` for normal operations.

## Service Directory Authority

[Gateway](../../glossary.md#gateway) nodes register services with the leader Coordinator through `register_gateway/4` or `register_services/2` operations. Service mappings replicate across all coordinators through consensus, providing Directors with complete topology information during [recovery](../../glossary.md#recovery).

## Director Lifecycle Management

Coordinator creates Directors with unique, monotonically increasing [epoch](../../glossary.md#epoch) numbers during leadership changes. This prevents split-brain scenarios while ensuring Directors receive populated service directories before starting recovery orchestration.

> **Complete Flow**: For cluster initialization and service registration sequences, see **[Cluster Startup Deep Dive](../../deep-dives/cluster-startup.md)**.

## Related Components

- **[Director](director.md)**: Recovery orchestration component created and managed by Coordinator
- **[Gateway](../infrastructure/gateway.md)**: Infrastructure component that registers services with Coordinator
- **[Foreman](../infrastructure/foreman.md)**: Infrastructure component coordinated through Coordinator's service directory
