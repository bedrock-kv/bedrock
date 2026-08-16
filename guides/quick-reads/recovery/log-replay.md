# Log Replay: Data Migration to Trusted Infrastructure

**Copying the committed WAL tail into a fresh generation of logs.**

Each new log receives the locked survivor set and rebuilds from any available
survivor. Every log holds the same encoded transaction stream, so the source
transaction binary is appended unchanged; the destination's Demux performs
normal shard slicing after the WAL append.

## One Range Convention

Planning supplies `{available_after, last_inclusive}` and object storage is
known durable through `durable_through`. Replay computes:

```text
replay_after = max(durable_through, available_after)
copy range   = (replay_after, last_inclusive]
```

Both lower inputs are exclusive cursors. `Log.pull/3` remains exclusive at its
start and inclusive at `last_version`, so there is no conversion at an API
boundary. A transaction at the first retained version is copied exactly once,
including when it is the only retained transaction. Numeric version gaps are
valid.

The destination records `replay_after` as logical WAL position without
creating a transaction. An empty range persists that baseline in a WAL segment
header. A non-empty range begins with the first real source transaction and
routes that exact binary through the fresh Demux.

Recovery succeeds only after observing `last_inclusive`. An empty page before
that endpoint is an incomplete replay, not evidence of success. Replay never
assigns the requested endpoint speculatively and never writes an empty
transaction as a progress marker.

## Why Restart Preserves the Boundary

Every WAL segment header stores the `previous_version` current when the segment
was created. The header and first entry are covered by the same fsync before the
old segment can become trim-eligible. Consequently, after trim and cold restart,
the oldest retained segment still states the exact exclusive cursor preceding
its data. Legacy headers without that fact fail closed.

New logs recover in parallel. Source unavailability is reported or retried
against another survivor; malformed, out-of-order, beyond-endpoint, or
incomplete data fails recovery.

**Prerequisites**: [Log recovery planning](log-recovery-planning.md),
[log recruitment](log-recruitment.md), and [service locking](service-locking.md)

**Next phase**: [Sequencer startup](sequencer-startup.md)

**Implementation**: `lib/bedrock/control_plane/director/recovery/log_replay_phase.ex`
