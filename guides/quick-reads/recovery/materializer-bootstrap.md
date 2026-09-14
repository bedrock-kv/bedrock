# Materializer Bootstrap

**Read the committed shard layout from a verified cache or rebuild that view from durable history.**

The shard boundaries live in `\xff/system/shard_keys/*`, served by tag 0.
The materializer serving that view is disposable: recovered logs and
object-storage chunks hold the history. Recovery must make the view readable
at its chosen recovery version before placing resolvers or routing writes.

## Fresh Cluster

A bootstrap record with no prior logs means there is no committed history.
Recovery creates a system materializer and seeds two shards: tag 0 for system
keys and tag 1 for user keys. Only this fresh path invents a layout.

## Existing Cluster

1. Prefer a locked tag-0 cache named by CoreState. Unlock it at the recovery
   version and let it resume its own stream cursor.
2. Wait until it can serve that version, then read the shard layout and
   materializer membership at the same version. Reuse the cache only if the
   committed family still names its worker and node.
3. If no named cache is available or the named cache was displaced, recruit a
   fresh tag-0 worker. Legacy records without cache hints use this path too.
   The worker starts from a committed snapshot or zero and streams old chunks
   followed by the recovered WAL suffix through its ShardServer.
4. Wait for catchup and read the existing layout and membership. An empty
   layout or failed read stalls recovery; it never becomes a fresh layout.
5. Carry any new worker into the recovery system transaction so its read
   coverage is published only after the historical reads succeed.

The distributor owns committed read coverage between recoveries and may
retire a CoreState-named cache. CoreState's log identities remain durable
recovery input; its materializer names are only a cache preference. No atomic
write across the keyspace and bootstrap object is required to keep those
preferences usable. An arbitrary worker claiming tag 0 is not substituted
for a missing named cache: reconstruction starts a worker with known replay
provenance instead.

## Failures

Missing capacity, failed worker startup, unavailable history, catchup timeout,
and failed or empty layout reads stall with their cause. A newly created
worker that fails before publication is removed. Potentially published workers
are reconciled against committed membership, so a lost persistence reply is
never treated as permission to destroy them.

## Next Phase

Recovery proceeds to [commit proxy startup](proxy-startup.md) with the recovered
shard layout and system read coverage.

**Implementation**: `lib/bedrock/control_plane/director/recovery/materializer_bootstrap_phase.ex`
