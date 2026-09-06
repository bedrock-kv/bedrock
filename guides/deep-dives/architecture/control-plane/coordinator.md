# Coordinator

The [Coordinator](../../../glossary.md#coordinator) maintains cluster state through [Raft](../../../glossary.md#raft) distributed [consensus](../../../glossary.md#consensus) and orchestrates [Director](../../../glossary.md#director) lifecycle during leadership changes. It serves as the authoritative source for cluster configuration and service directory information, enabling distributed coordination across all nodes.

**Location**: [`lib/bedrock/control_plane/coordinator.ex`](../../../lib/bedrock/control_plane/coordinator.ex)

## Consensus Leadership

Coordinators use Raft consensus to maintain cluster configuration and service directories. The leader Coordinator persists changes through consensus, ensuring consistent state replication across all coordinator nodes and automatic failover during leader elections.

Leader readiness states prevent race conditions: `:leader_waiting_consensus` delays [Director](../../../glossary.md#director) startup until service directory population completes, then transitions to `:leader_ready` for normal operations.

## Service Directory Authority

[Link](../../../glossary.md#link) nodes register services with the leader Coordinator through `register_link/4` or `register_services/2` operations. Service mappings replicate across all coordinators through consensus, providing Directors with complete topology information during [recovery](../../../glossary.md#recovery).

## Director Lifecycle Management

Coordinator currently derives Director [epoch](../../../glossary.md#epoch) numbers from Raft leadership terms. A Director restarted within the same term still reuses that epoch; durable recovery-generation allocation remains tracked in [#259](https://github.com/bedrock-kv/bedrock/issues/259) and [#296](https://github.com/bedrock-kv/bedrock/issues/296). Notification instance attribution below prevents retired Directors from replacing cached state, but does not provide the remaining storage and publication fences.

> **Complete Flow**: For cluster initialization and service registration sequences, see **[Cluster Startup Deep Dive](../../../deep-dives/cluster-startup.md)**.

## Related Components

- **[Director](director.md)**: Recovery orchestration component created and managed by Coordinator
- **[Link](../infrastructure/link.md)**: Infrastructure component that registers services with Coordinator
- **[Foreman](../infrastructure/foreman.md)**: Infrastructure component coordinated through Coordinator's service directory


### Director notification identity

A Director tags each config or completed-layout notification with its PID,
recovery epoch and an increasing process-local sequence. The Coordinator accepts
only its registered Director, bound to the Raft term in which it was launched.
It checks the actual Raft leadership as well as the cached leadership: an RPC can
change Raft authority before the queued leadership callback updates that cache.
A newer term retires the old instance even when the same node remains leader.

Config and layout have separate sequence high-water marks. Duplicates and older
messages cannot replace state or rebroadcast a layout, while a newer config
arriving before a layout does not discard that layout. Notifications do not
assign the Coordinator epoch. Retiring the Director clears and broadcasts the
unavailable layout, preserving the durable prior core state for recovery.

This is an internal notification protocol, not a durable recovery-generation or
object-storage publication fence. Config notifications also carry stalled
recovery progress. Unattributed legacy messages are ignored; Coordinators and
Directors must use compatible binaries for publication to work.
