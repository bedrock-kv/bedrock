# Materializer

A [Materializer](../../../glossary.md#materializer) turns a shard's
committed transactions into queryable, versioned key-value state and
serves reads from it. It holds one shard, streams that shard's slices
from a log's Demux, and answers `get` at a version.

**Location**: [`lib/bedrock/data_plane/materializer.ex`](../../../../lib/bedrock/data_plane/materializer.ex)

## Why the read path is a separate component

A committed transaction is durable the moment the logs acknowledge it,
but a log is an append-only record. Answering "what is the value of key
`k` at version `v`" from a log means replaying it, so Bedrock keeps a
component whose whole job is to hold the answer already computed.

FoundationDB solves this with storage servers, and the resemblance is
real — but the ownership model is where Bedrock diverges, and it is worth
being precise about how.

## The deviation from FoundationDB: no teams, zero or more members

In FoundationDB a shard is maintained by a **server team**: *k* storage
servers, *k* being the replication factor. The team *is* the durable copy.
Data distribution exists largely to keep teams healthy — when a member
fails, DD issues a `RelocateShard` to rebuild the replication factor by
copying data from one server to another, and every shard belongs to a
team at all times.

Bedrock has no teams, and no replication factor among materializers.
That is not because a materializer matters less — clients need exactly
what it holds, and it is the whole read path — but because nothing in it
exists only there. Every byte derives from the logs and from the
[chunks](../../../glossary.md#chunk) their Demux writes to
[object storage](../../../glossary.md#object-storage), which is where
durability lives. A materializer is a view of that record rather than
part of it.

So the question a replication factor answers — how many copies must
survive for the data to survive — does not arise. Losing a materializer
costs the work of rebuilding one, and nothing else.

The consequence is that a shard has **zero or more** materializers at any
moment, and the number changes during normal operation:

- The [Distributor](../../../glossary.md#distributor) recruits a
  materializer for a tag on demand and publishes it into the
  `materializers/<tag>/` family in the system keyspace
- A materializer can retire itself when idle: it uploads a snapshot and
  is removed, leaving the tag with one fewer member. This is opt-in per
  worker and nothing in the shipped configuration sets the parameter, so
  today the count shrinks through failure healing rather than through
  idleness
- When a tag has no materializer at all, the Distributor publishes a
  [Placeholder](../../../glossary.md#placeholder): an ordinary member of
  the set that speaks the read API and *parks* reads rather than serving
  them, shedding `{:error, :unavailable}` — which clients already treat
  as retryable — if coverage does not arrive in time

Losing a materializer therefore loses nothing but a cache. There is no
team to repair, no shard to relocate, and no data movement between
members; a replacement is recruited and rebuilds from the durable
record.

## How a materializer gets its data

A materializer never reads the WAL, and never reads
[chunks](../../../glossary.md#chunk) itself — those reach it through the
log's [ShardServer](../../../glossary.md#shardserver), as one continuous
stream. (An implementation that keeps a durable baseline may read object
storage directly for that one thing: Olivine downloads its own snapshot on
cold start, then joins the stream from there.)

1. A commit reaches the logs, which push it to their
   [Demux](../../../glossary.md#demux)
2. The Demux slices each transaction by shard and routes the slice to
   that shard's ShardServer
3. The ShardServer buffers slices in memory, and on a Demux-commanded
   [cut](../../../glossary.md#cut) writes everything at or below the cut
   version to object storage as one chunk
4. A materializer calls `pull/3` from one past the version it already
   holds. Where the reply comes from is one comparison against the last
   confirmed cut: at or below it, the chunk range in object storage;
   above it, the buffer

The two regions always meet, because a buffered entry is only evicted
once its chunk write confirms. That is what makes the stream continuous
from any starting position, and why a materializer that has been offline
simply has more stream to drink rather than a special catch-up path. Its
only contact with a log is discovery — `Log.get_shard_server/2`, once per
session and again on failover.

Every reply carries currency: `%{high_water: v, kcv: k}`. An empty reply
still says something — "nothing for you, but you are current through
`v`" — so a materializer whose shard is idle keeps advancing its version,
and keeps serving fresh reads, without polling.

## Versions and their bounds

A materializer serves reads at a version, which is what lets a
transaction see a consistent snapshot while writes continue underneath
it. Three outcomes are possible, and they are part of the role rather
than of any implementation:

- the version is available, and the read is answered
- the version has not been applied yet, so the read parks briefly and
  then answers `{:error, :version_too_new}`
- the version is older than the implementation still keeps, and the read
  answers `{:error, :version_too_old}`

Neither miss reaches the caller. Both are classified retryable, and a
transaction that hits one restarts against a fresh read version. Reads are
addressed by version and cannot return stale state, so the cost of
crossing a bound is a retry, not a wrong answer.

*How far back* a materializer can reach is a policy of the implementation
and of how it was configured at boot, not a property of the role. Olivine
keeps a sliding window whose width is a boot parameter, defaulting to five
seconds; a different implementation could keep more, less, or something
shaped differently.

Conflict detection does not depend on any of this. That is the
[Resolver](../../../glossary.md#resolver)'s work, done against conflict
ranges it keeps itself; resolvers never query a materializer.

## Recovery: a cache, never a source of truth

Because the durable record is the log plus its chunks, a materializer is
never a point of failure for durability, and can be rebuilt from that
record.

What recovery asks of the role is narrow: on `lock_for_recovery/2` a
materializer reports the version it holds, and on
`unlock_after_recovery/5` it is told where to resume and given its pull
sources. The obligation behind that report is the important part — a
materializer must never claim a version that a recovery could discard.

Nothing not known-committed ever becomes durable in object storage, so
recovery needs no chunk cleanup either: the uncommitted tail lives only in
ShardServer buffers, which die with the Demux tree.

How an implementation keeps its own claim honest is its business. Olivine
applies transactions eagerly for read currency but persists only up to the
known committed version, which makes a rollback a pointer discard rather
than disk surgery. An implementation that never touched disk would satisfy
the same obligation trivially.

## Reading: ask, do not pre-confirm

A transaction acquires a read version from the
[Sequencer](../../../glossary.md#sequencer) on its first read, and from
then on it simply **asks** — there is no step that confirms in advance
that a materializer has reached that version, and there could not usefully
be one. Membership is a set that changes, members lag independently, and
a pre-flight check across them would put a synchronous barrier on the
critical path of every read to buy a guarantee that is stale the moment
it is given.

Instead, currency is discovered by asking. A member that has not applied
the read version yet parks the request briefly and then answers
`{:error, :version_too_new}`; one whose window has already moved past it
answers `{:error, :version_too_old}`. Both are retryable, and neither
reaches the caller.

### The race

This is what racing is for, and it is not only about speed. Members of a
shard's set are **not replicas of a durable copy** — each streams the
shard independently and applies at its own pace, so at any instant they
sit at different versions. A client holding a read version tries the
members it has, takes the first successful answer, and caches that member
as the fastest for the key range; subsequent reads in the transaction go
straight to it.

So a race settles two questions at once: which member is quickest, and
which is far enough along to answer at all. The second has no counterpart
in FoundationDB, where every member of a team holds the data and racing is
purely a tail-latency hedge.

When the cached-fastest member comes back slow, unavailable, or
`:version_too_new`, the read races the remaining members and re-caches the
winner. `:version_too_old` is the exception: it fails straight out rather
than racing, because a version below the retention window is below it
everywhere.

### What ships today

The commit proxy currently resolves a key to exactly one member —
`pick_member/1`, deterministic, with a
[Placeholder](../../../glossary.md#placeholder) deprioritized so real
coverage always wins — and hands the builder a single ref. That keeps
every proxy answering alike and a retry landing consistently, and it means
the special case for an uncovered shard lives in the proxy rather than in
the reader.

With one runner the race has nothing to choose between. A failure surfaces
as `:no_servers_to_race`, which invalidates the cached route, so the retry
re-resolves and may land on a different member — the retry loop doing
coarsely what a race does directly. Placing several materializers per
shard by load and locality is its own piece of work; the selection rule
belongs here when it arrives.

## The role and its implementations

Everything above describes a **role**, and the role is the abstraction
that matters. `Bedrock.DataPlane.Materializer` is its surface: `get/4` and
`get_range/5` for versioned reads, `lock_for_recovery/2` and
`unlock_after_recovery/5` for the recovery handshake — the latter carrying
the pull sources a materializer streams from — and `info/3` for the facts
recovery and operators ask of it.

Anything that answers those calls honestly is a materializer. Nothing in
the role says how state is represented, whether it touches disk at all,
or how far back it can reach. An implementation that kept a fast in-memory
window and never persisted a byte would be a perfectly good materializer:
the durable record is elsewhere, so persistence is an optimisation for
rebuild cost, not a requirement of the role.

[Olivine](../implementations/olivine.md) is the implementation in use, and
the only one today — the Foreman starts it for every `:materializer`
worker. Its page index, its retention window, its disk format and its
snapshot handling are Olivine's own design, described there rather than
here. Where this page names a concrete number, it is Olivine's default and
is marked as such.

## Related Components

- **[Olivine](../implementations/olivine.md)**: the implementation of the role in use today, and where its internals are described
- **[Log System](log.md)**: hosts the Demux and ShardServers a materializer streams from
- **[Distributor](../../../glossary.md#distributor)**: recruits materializers, publishes coverage, and parks uncovered tags behind a [Placeholder](../../../glossary.md#placeholder)
- **[Transaction Builder](../infrastructure/transaction-builder.md)**: races a shard's members for reads
- **[Object Storage](../../../glossary.md#object-storage)**: holds the chunks and snapshots a materializer is rebuilt from
