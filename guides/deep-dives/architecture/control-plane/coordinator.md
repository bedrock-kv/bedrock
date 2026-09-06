# Coordinator

The [Coordinator](../../../glossary.md#coordinator) maintains cluster state through [Raft](../../../glossary.md#raft) distributed [consensus](../../../glossary.md#consensus) and orchestrates [Director](../../../glossary.md#director) lifecycle during leadership changes. It serves as the authoritative source for cluster configuration and service directory information, enabling distributed coordination across all nodes.

**Location**: [`lib/bedrock/control_plane/coordinator.ex`](../../../lib/bedrock/control_plane/coordinator.ex)

## Consensus Leadership

Coordinators use Raft consensus to maintain cluster configuration and service directories. The leader Coordinator persists changes through consensus, ensuring consistent state replication across all coordinator nodes and automatic failover during leader elections.

Leader readiness states prevent race conditions: `:leader_waiting_consensus` delays [Director](../../../glossary.md#director) startup until service directory population completes, then transitions to `:leader_ready` for normal operations.

## Service Directory Authority

[Link](../../../glossary.md#link) nodes register services with the leader Coordinator through `register_link/4` or `register_services/2` operations. Service mappings replicate across all coordinators through consensus, providing Directors with complete topology information during [recovery](../../../glossary.md#recovery).

## Director Lifecycle Management

Each Director receives a recovery generation allocated by a committed Raft command. This generation is separate from the leadership term: two Director failures under one leader consume distinct generations. The immutable Director binding still records its owning Raft term for notification attribution.

Before allocation, the leader commits a current-term barrier, reads coherent bootstrap bytes and their version token, and proposes an explicit generation above both the replicated floor and the bootstrap reservation. Applying that command never consults object storage: replay of the same committed prefix reconstructs the same floor. A single synced DETS checkpoint contains the applied cursor and matching state. Startup verifies it against the retained committed prefix, rejects gaps or contradictions, and replays the remaining suffix without launching historical Directors.

Allocation alone cannot launch recovery. The Coordinator must reserve that generation by conditional write to the same bootstrap object that the Director will later publish. Reservation retains the previous completed log set and configuration. The Director receives those exact prior bytes and an immutable reservation token. A newer reservation invalidates every older final write token.

Recovery I/O runs outside the Coordinator process with a five-second deadline (configurable through `:recovery_io_timeout_ms`). Completion must still match the pending request and actual leadership. Timeout, task failure, and leadership loss revoke local authority without waiting for native I/O to stop. A subsequent recovery trigger consumes a new generation; an abandoned external write is never undone.

Shale ownership and live-push fencing remain tracked in [#297](https://github.com/bedrock-kv/bedrock/issues/297), with the combined fault-history audit in [#259](https://github.com/bedrock-kv/bedrock/issues/259).

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

Completed-layout notifications additionally carry the reserved publication identity.
The Coordinator acknowledges accepted layouts, including exact duplicate delivery,
without rebroadcasting duplicates. The Director sends the same sequence and payload
at most three times, one second apart; missing acknowledgment then terminates it.
Config notifications also carry stalled recovery progress and do not modify durable
bootstrap authority. Unattributed legacy messages are ignored.

### Quiescent protocol upgrade

Stop all old Coordinators and Directors before activating protocol version 1.
Mixed-version writers are unsupported: an old writer can overwrite fields it does
not understand, so wire-message rejection alone cannot make a rolling upgrade safe.
Keep the object-store namespace and complete Coordinator Raft history. Activation
requires a durable Coordinator path and a conditional-write backend.

A legacy completed bootstrap has positive epoch and a nonempty log set. Its first
reservation preserves that completed metadata even though no publication identity
was recorded by the old format. A bootstrap with no logs at epoch zero or one is
still a fresh cluster; reserving a later generation does not manufacture a completed
recovery. Malformed identities, duplicate members, inconsistent generation/publication
fields, and unknown format versions fail closed before activation. Do not edit or
clear the generation checkpoint to bypass an error; repair requires the matching
committed history and bootstrap authority.
