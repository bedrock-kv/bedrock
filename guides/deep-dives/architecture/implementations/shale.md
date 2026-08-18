# Shale

[Shale](../../../glossary.md#shale) is Bedrock's local-disk transaction log.
It provides ordered, fsync-durable transaction storage, exclusive range pulls
for log recovery, and a Demux tree that turns the replicated transaction stream
into per-shard object-storage chunks.

**Location**: `lib/bedrock/data_plane/log/shale/`

## WAL Segments

Shale writes into preallocated 64 MiB segment files. A segment rolls when it is
full or when a transaction crosses the Demux's deterministic cut boundary. The
active segment is never trimmed; completed segments can be recycled once their
last transaction is at or below the replica-local object-storage durability
floor.

The versioned `BED1` segment header contains the eight-byte
`previous_version` that was the WAL tip when the segment was created. Entries
then contain the commit version, payload length, original encoded transaction,
and CRC32, followed by an EOF marker. The header is twelve bytes:

```text
BED1 | previous_version
```

The first append writes the entry and EOF marker and fsyncs them together with
the header before acknowledging. Thus an older segment cannot become
trim-eligible before its successor durably records the exact predecessor
cursor. An empty recovery range explicitly fsyncs the header by itself.

Headers that predate `BED1` do not contain enough information to reconstruct a
trimmed exclusive range. Startup rejects them instead of guessing a cursor.

## Ordered Appends

A push names the previous committed version. If it matches the WAL tip, Shale
appends immediately. If it is greater, Shale parks the transaction until its
predecessor arrives; if it is older, Shale rejects it. Commit versions may have
arbitrary numeric gaps—the explicit predecessor chain defines order.

An append is acknowledged only after WAL fsync. The exact encoded binary that
was appended is then passed unchanged to Demux. Demux alone slices mutations by
shard, so crossing the process boundary does not require rebuilding the
transaction binary.

Known committed version (KCV) is a separate global monotonic watermark. Demux
accumulates it with `max`, including while a future transaction is parked, but
does not confuse KCV progress with transaction high-water.

## Pull and Availability Semantics

`Log.pull/3` returns transactions in `(start_after, last_inclusive]`. Recovery
is its only WAL consumer; materializers stream from ShardServers instead.

Shale exposes both:

- `available_after`: the persisted exclusive cursor after which all retained
  WAL transactions are available;
- `oldest_version`: informational data about the first retained transaction.

They are intentionally different. Using `oldest_version` as an exclusive
cursor would skip the transaction it names.

## Recovery

Cold start enumerates segment files, reads every `BED1` predecessor, and derives
`available_after` from the oldest retained segment. The WAL tip is the newest
transaction, or the header cursor for an empty baseline.

Log-to-log recovery has one range: `(replay_after, last_inclusive]`. It resets
the destination to logical position `replay_after` without appending a sentinel,
copies each real transaction byte-for-byte, and sends it through a fresh Demux
exactly once. Success requires observing `last_inclusive`; an empty response
before it is an error. When the range is empty, Shale persists only a header
baseline, which survives restart and anchors the next push.

## WAL Trimming

Each Demux commands version-time cuts gated by KCV. Its ShardServers confirm a
cut only after their deterministic chunks are present in object storage. The
minimum confirmation from that log's own children advances its trim floor.
Shale recycles only completed segments fully behind the floor and recomputes
`available_after` from the oldest segment that remains.

Trim-floor telemetry reports lag and segment counts. Growth is
unbounded-with-alerting by default. The optional hard limit is an epoch-fatal
safety fuse, not retryable per-push backpressure. Shale checks each prospective
commit version, including queued successors when a predecessor gap closes. If
the version-time lag exceeds the limit, every affected caller is released with
`{:error, {:recovery_required, {:wal_limit_exceeded, details}}}` and the commit
proxy stops so the Director can recover the known-committed prefix. Continuing
the epoch would be unsafe because the sequencer and resolvers have already
incorporated the refused version, and some log replicas may already have
fsynced it.

The fuse emits `[:bedrock, :log, :wal_limit_exceeded]` at error severity with
the floor, current tip, prospective version, lag, configured limit, and queued
count. Recovery replay is exempt because it copies history already selected as
committed.

## Code References

- `writer.ex`: versioned headers, entry encoding, fsync
- `segment.ex` and `cold_starting.ex`: segment metadata and restart
- `pushing.ex` and `pulling.ex`: predecessor-chain appends and exclusive pulls
- `recovery.ex`: endpoint-proven log-to-log replay
- `server.ex`: lifecycle, facts, durability-floor trimming
