# Materializer Bootstrap

**Get the shard layout back, by asking the materializer that already holds it.**

Recovery needs to know the cluster's shard boundaries before it can place
resolvers or route anything. Those boundaries live in the committed keyspace
under `\xff/system/shard_keys/`, which means recovery has to bring one
materializer — the one serving the system shard, tag 0 — back online and read
them out of it.

This phase is deliberately *not* a recruitment phase. Materializers hold
irreplaceable committed state, so the goal is to reuse the survivors, not to
replace them.

## Fresh Cluster

With no prior logs there is nothing to recover, so the phase invents the
starting layout: two shards, the system shard (tag 0) covering `0xFF` to the
end of the keyspace, and the user shard (tag 1) covering everything below
`0xFF`.

## Existing Cluster

1. **Resolve the system materializer by name** from the prior
   [`CoreState`](../transaction-system-layout.md). It is looked up, never
   invented — recovery does not guess which worker was serving tag 0.
2. **Lock it** for this recovery.
3. **Unlock it** with its replica set of pull sources, so it starts pulling
   from the logs.
4. **Wait for catchup** — up to 60 seconds, polling — until it has applied
   through the recovery version.
5. **Read the shard layout** from `\xff/system/shard_keys/*`.

The `\xff/system/materializers/` family is read alongside it: a family-named
worker that this epoch locked, whose own shard assignment agrees, is
re-adopted for its shard. Only a shard with no survivor gets a fresh
materializer, which rebuilds from its object-storage chunks.

## Stalls

The phase stalls — and recovery retries — if the named members are
unavailable, if catchup times out, or if the recovered layout reads empty. An
empty read is treated as a failure rather than as "this cluster has no
shards", because the difference matters: see
[The System Keyspace](../system-keyspace.md).

## Next Phase

Recovery proceeds to [commit proxy startup](proxy-startup.md) with the system
materializer's pid and the shard layout in hand.

---

**Implementation**: `lib/bedrock/control_plane/director/recovery/system_shard_bootstrap_phase.ex`
