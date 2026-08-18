# Log Recovery Planning

**Determining the common recoverable WAL range without confusing data with a cursor.**

Bedrock writes every committed transaction to every log. Recovery can proceed
when a majority of the previous generation's logs can be locked. Those
survivors report three distinct positions:

- `available_after`: an exclusive cursor; every retained transaction after it
  is available from that WAL.
- `last_inclusive`: the last transaction version present in the WAL (reported
  as `last_version`).
- `minimum_durable_version`: the transient object-storage durability
  watermark, or `:unavailable` after restart.

`oldest_version` remains useful for inspection, but it names retained data and
is never used as an exclusive pull cursor.

## Common Range

Across the locked majority, planning computes:

```text
available_after = max(each survivor's available_after)
last_inclusive  = min(each survivor's last_version)

common range = (available_after, last_inclusive]
```

The lower bound is deliberately exclusive. A survivor whose first retained
transaction is version 10 can report `available_after = 9`; replay from 9 then
includes version 10 exactly once. Versions may have arbitrary numeric gaps, so
the cursor is persisted rather than derived by subtracting from the first
transaction.

Planning separately takes the minimum available durability watermark. Log
replay may advance its exclusive cursor to that point because object storage is
durable through it. The watermark is an optimization, not the WAL's persisted
availability contract.

If the locked logs do not form a majority, or if the aggregated lower cursor is
greater than the inclusive endpoint, recovery stalls with
`:unable_to_meet_log_quorum`.

## Outputs

- `survivor_log_ids`: the locked logs that can serve as replay sources.
- `version_vector`: `{available_after, last_inclusive}`.
- `durable_version`: the common `durable_through` optimization.
- Fresh log vacancies for the next generation.

Success leads to [log recruitment](log-recruitment.md), followed by
[log replay](log-replay.md).

**Implementation**: `lib/bedrock/control_plane/director/recovery/log_recovery_planning_phase.ex`
