# Distributed Durability Test Suite

Bedrock includes a MinIO-backed distributed durability suite for foundational
3-shard scenarios.

Test module:

- `test/bedrock/distributed/minio_durability_test.exs`

Tags:

- `:distributed`

## Local Execution

1. Ensure MinIO test binaries are installed:

```bash
MIX_ENV=test mix minio_server.download --arch darwin-arm64 --version latest
```

2. Run the distributed suite explicitly:

```bash
BEDROCK_INCLUDE_DISTRIBUTED=1 mix test --include distributed test/bedrock/distributed/minio_durability_test.exs
```

By default, `:distributed` tests are excluded unless explicitly enabled.

## CI Execution

The CI workflow runs distributed durability tests in a dedicated job on
scheduled/manual runs with MinIO setup. This keeps default PR pipelines fast
while preserving regular distributed durability coverage.

## Transaction histories against the real core

The transaction-history harness starts real coordinators, sequencers, commit
proxies, resolvers, logs, and materializers. It submits transactions through the
public Repo API and checks their observations and final state against an
independent in-memory interpreter. It does not use Bedrock's mutation or conflict
helpers to calculate expected results.

Run all history scenarios without MinIO:

```bash
MIX_ENV=test elixir --sname bedrock_history -S mix run --no-start scripts/transaction_history.exs
```

The runner includes the oracle unit tests, local transaction histories, snapshot
publication crashes, and three-node peer scenarios. A named Erlang node and local
peer-process/distribution access are required. These tests remain tagged
`:distributed` and excluded from the ordinary test run. The existing MinIO CI job
does not automatically run this standalone command.

Workloads cover batched atomic increments, ordered mutations, half-open clears,
read-your-writes over an otherwise empty range, conditional reservations based on
absence, and transfers based on point reads. Barriers make conflicting reads and
shared proxy batches observable. Each attempt records its invocation, completion,
operations, partial reads, callback completion, and outcome. Retries have distinct
IDs; a timeout after a possibly committed callback is recorded as an unknown outcome
rather than proof of an abort.
The bounded checker searches legal serial orders, preserves real-time order for
known completed attempts, and permits each unknown attempt to commit at most once
or not at all. The final state must match exactly. It rejects oversized or invalid
histories rather than silently accepting them.

Fault schedules exercise:

- Log loss before append and after the real WAL sync, before the reply. The log
  and coordinator are crashed together; recovery must advance the epoch and
  preserve acknowledged transactions.
- Materializer loss immediately before and after snapshot publication to real
  filesystem object storage. The fixture establishes a durable version below the
  applied tail, checks the published snapshot against that exact prefix, starts
  a replacement with empty local storage, and checks tail replay exactly once.
- Three actual BEAM nodes with distinct component roles. One schedule severs and
  verifies all distribution edges across the log-node cut; another stops and
  restarts that node with its WAL preserved. Both recover through a coordinator
  restart and check current-epoch shard coverage before checking the history.

These are bounded, repeatable fault schedules, not a deterministic BEAM simulator
or a quorum-availability test. The fixtures use replication factor one and retain
log disks. Coordinator restart is deliberate: autonomous director-only recovery
currently reuses an epoch and can stall after log loss, tracked in
[issue #259](https://github.com/bedrock-kv/bedrock/issues/259). To retain that failing
schedule for investigation, run the command above with
`BEDROCK_HISTORY_SINGLE_LOG_REPRO=1`; it is expected to fail until that issue is
resolved. Snapshot replacement waits for the foreman's automatic restart health
before eviction; the separate immediate-eviction race is tracked as `bedrock-gu0.5`.

## History artifacts and replay

Every scenario writes a binary Erlang term, including on assertion failure.
Local and snapshot scenarios also write a readable `.term.txt` companion. The output prints their paths. Set
`BEDROCK_HISTORY_ARTIFACT_DIR` to retain them in a chosen directory. Artifacts
include source revision, seeds, attempt histories, fault/epoch evidence, and final
state when reached. An unfinished attempt remains visible as `:in_flight` with
its partial observations, so an early failure does not erase its evidence.

Set `BEDROCK_HISTORY_SEED` to repeat ExUnit ordering (default `239`). Mixed mutation
workloads use explicit seeds `239`, `240`, and `241`. Replaying repeats workload
choices and controlled boundaries; operating-system process timing can differ.
Read a trusted local artifact from IEx with:

```elixir
history = path |> File.read!() |> :erlang.binary_to_term()
IO.inspect(history, pretty: true, limit: :infinity)
```

The four tests tagged `:counterexample` reproduce the original atomic batching,
clear endpoint, pending range visibility, and absence-conflict failures on the
pre-fix core. Snapshot tests separately reject a snapshot containing the applied
suffix beyond its advertised durable version. Keep these failures as concrete
checks that the harness detects corruption rather than merely cluster liveness.
