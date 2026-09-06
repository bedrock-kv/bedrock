# Transaction System Layout

**This epoch's wiring — and nothing else.**

After recovery brings individual components online, they need to know how to
reach each other. The Transaction System Layout (TSL) carries exactly that:
the process references a transaction needs to find the sequencer, a commit
proxy, the resolvers, and the logs.

It is deliberately small. The TSL is FoundationDB's `ServerDBInfo` — *"transient
information which is broadcast to all workers for a database, permitting them
to communicate with each other"* — and it is republished on every recovery, to
every node. So nothing that grows with the cluster may ride on it.

## What is not in it

Shard topology and worker membership used to live here and no longer do. They
are durable facts about the cluster, so they live in the committed keyspace
instead (see [The System Keyspace](system-keyspace.md)):

- **Shard boundaries** are `\xFF/system/shard_keys/`, and clients resolve them
  one key at a time through a commit proxy — FDB's `GetKeyServerLocations` —
  rather than receiving the whole map in a broadcast.
- **Materializer membership** is `\xFF/system/materializers/`, read by routing
  and by a worker validating its own rejoin.
- **The durable record** the next recovery must find is `CoreState`, kept in
  object storage — FDB's `DBCoreState`, as against `ServerDBInfo`.

The rule this encodes: the broadcast carries wiring, the keyspace carries
state. Nothing O(workers) may be added back to the TSL.

## Fields

- **`epoch`** — the recovery generation this wiring belongs to. Every call
  carries it, and a mismatch means the caller is talking to a dead epoch.
- **`sequencer`** — the pid that hands out read and commit versions.
- **`proxies`** — the commit proxy pids: commit entry points, and the servers
  that answer per-key routing.
- **`resolvers`** — `%{start_key: ..., resolver: pid}` descriptors, ordered by
  start key, consumed when a proxy unlocks.
- **`logs`** — a map of log id to the list of shard tags that log services.

## When created

Recovery builds the layout once all components are locked and started, then
publishes it. Workers receive it through their foreman, which relays rather
than reconciles — each worker decides its own retirement from it.

## Quick reference

The layout is a plain map, not a struct:

```elixir
# From a client node
{:ok, layout} = Bedrock.Cluster.Link.fetch_transaction_system_layout(link)

# Or straight from the director
{:ok, layout} = Bedrock.ControlPlane.Director.fetch_transaction_system_layout(director)

%{
  epoch: 5,
  sequencer: #PID<0.124.46>,
  proxies: [#PID<0.125.47>, #PID<0.127.49>],
  resolvers: [
    %{start_key: "", resolver: #PID<0.128.50>},
    %{start_key: "m", resolver: #PID<0.129.51>}
  ],
  logs: %{"log_a" => [0, 1], "log_b" => [2, 3]}
}
```

## Implementation

- **Main Module**: `lib/bedrock/control_plane/config/transaction_system_layout.ex`
- **Durable Counterpart**: `lib/bedrock/control_plane/config/core_state.ex`
- **Assembly**: `lib/bedrock/control_plane/director/recovery/topology_phase.ex`
- **Validation**: `lib/bedrock/control_plane/director/recovery/core_state_validation_phase.ex`
- **Publication**: `lib/bedrock/control_plane/director/recovery/persistence_phase.ex`

## See Also

- [The System Keyspace](system-keyspace.md) - Where shard topology and membership live
- [Recovery](recovery.md) - How the layout gets built
- [Transactions](transactions.md) - How the wiring is used
