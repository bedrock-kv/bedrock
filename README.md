# Bedrock

[![Elixir CI](https://github.com/bedrock-kv/bedrock/actions/workflows/elixir_ci.yaml/badge.svg)](https://github.com/bedrock-kv/bedrock/actions/workflows/elixir_ci.yaml)
[![Coverage Status](https://coveralls.io/repos/github/bedrock-kv/bedrock/badge.png?branch=develop)](https://coveralls.io/github/bedrock-kv/bedrock?branch=develop)
[![Hex.pm](https://img.shields.io/hexpm/v/bedrock.svg)](https://hex.pm/packages/bedrock)

Bedrock is a distributed key-value store that runs *inside* your Elixir
application. It takes the transaction architecture that made
[FoundationDB](https://apple.github.io/foundationdb/) legendary — strictly
serializable ACID transactions across the entire key-space, over an unbundled
sequencer / log / storage design — and rebuilds it natively on the BEAM, with
one modern twist: **durable history lives in object storage.**

Local disks hold only a few seconds of write-ahead log. Everything committed
flows into immutable chunks in S3-compatible storage (or the local
filesystem), and the WAL continuously trims itself behind that durable floor.
There is no external database to operate and no fleet to manage: add a
dependency, join your supervision tree, and every node of your cluster shares
one transactional key-space.

```elixir
def deps do
  [
    {:bedrock, "~> 0.7"}
  ]
end
```

Bedrock requires Elixir 1.17 or later. CI tests OTP 27, 28, and 29 with
Elixir 1.17.3, 1.19.5, and 1.20.3 respectively; see the
[CI matrix](.github/workflows/elixir_ci.yaml) for the exact versions.

## A taste

Define a cluster and a repo, add them to your supervision tree, and you have a
database:

```elixir
defmodule MyApp.Cluster do
  use Bedrock.Cluster, otp_app: :my_app, name: "my_app"
end

defmodule MyApp.Repo do
  use Bedrock.Repo, cluster: MyApp.Cluster
end
```

Transactions read from a consistent snapshot, see their own writes, and either
commit atomically or not at all — across any keys, on any node:

```elixir
alias MyApp.Repo

Repo.transact(fn ->
  balance = Repo.get("alice/balance")
  Repo.put("alice/balance", debit(balance, 10))
  Repo.put("bob/balance", credit(balance, 10))
  {:ok, :transferred}
end)
```

Familiar FoundationDB layers come along for the ride: `Bedrock.Directory` and
`Bedrock.Keyspace` for organizing data, key selectors, range reads, and atomic
operations (`add`, `min`, `max`, `append_if_fits`, `compare_and_clear`, ...)
for building higher-level abstractions without read-write conflicts.

**Try it live:** the class-scheduling tutorial (adapted from FoundationDB's
classic) builds a working enrollment system in a notebook — no setup beyond
clicking the badge.

[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2Fbedrock-kv%2Fbedrock%2Frefs%2Fheads%2Fdevelop%2Flivebooks%2Fclass_scheduling.livemd)

## Why Bedrock

**The FoundationDB model, kept.** A single sequencer hands out global
versions (a Lamport clock), commit proxies batch transactions, resolvers
detect MVCC conflicts over key ranges, logs make commits durable, and
materializers serve reads — each a separate, independently recoverable
process. Optimistic concurrency means no locks and no deadlocks; conflicts
are detected at commit time. The result is strict serializability: the
strongest isolation guarantee a database can offer. FoundationDB's
[architecture overview](https://apple.github.io/foundationdb/architecture.html),
[read/write path](https://apple.github.io/foundationdb/kv-architecture.html),
and [SIGMOD '21 paper](https://www.foundationdb.org/files/fdb-paper.pdf) are
excellent background on the design; Bedrock's own
[deep dives](guides/deep-dives/architecture.md) cover how it maps onto the
BEAM and where it diverges.

**Durability, modernized.** A commit is acknowledged only after *every*
required log has appended it to its write-ahead log and fsynced. From there,
a known-committed-version watermark gates everything downstream: nothing
enters object storage or a materializer's disk unless it is known to be
committed, so recovery never has to undo derived state. Every five seconds,
each log slices its recent transactions per shard and writes them to object
storage — and because every replica sees the same versions and computes the
same cuts, replicas produce *byte-identical* chunks, so "that file already
exists" counts as success. Once chunks are confirmed, the WAL trims itself: a
log's disk footprint tracks a few seconds of traffic, not the cluster's
lifetime.

**Restarts that don't scale with history.** Recovery replays only the
untrimmed WAL tail — seconds of transactions — never the whole history.
Surviving materializers are handed back their shards and resume streaming
from where they stopped; only a genuinely lost shard rebuilds from chunks. A
typical restart converges in under a second regardless of how old the cluster
is.

**BEAM-native architecture.** Transaction processing, supervision and recovery
run on the BEAM without an embedded native storage engine or sidecar processes.
The LocalFilesystem backend uses a small POSIX NIF for atomic filesystem mutation
across independent VMs.
Distribution rides on Erlang distribution; supervision, recovery, and
backpressure are OTP all the way down. Bedrock also leans on a quiet BEAM
superpower: large binaries are shared between processes by reference, never
copied. Keys, values, and whole encoded transactions stay binaries as they
flow from commit proxy to log to shard streams, so the hot path hands around
pointers rather than payloads — a design that is naturally multi-core and
cache friendly.

Building Bedrock from source requires Linux or macOS, a C compiler, make and OTP
development headers, including for S3-only applications. On Linux install your
distribution's build-essential and Erlang development packages; on macOS install
Xcode Command Line Tools. Windows is not supported by this native build.
See [LocalFilesystem requirements](guides/local-filesystem.md) for filesystem,
upgrade and cross-compilation constraints.

## How a write becomes durable

1. A commit proxy pushes the transaction to every required log; the client is
   acknowledged only after each log has fsynced its WAL.
2. The sequencer tracks the highest version confirmed on every log — the
   known committed version — and everything downstream waits for it.
3. On five-second version boundaries, each log's shards flush everything at
   or below the cut to object storage as immutable, replica-identical chunks.
4. Confirmed chunks raise the durable floor; the log recycles every WAL
   segment that falls entirely below it.
5. Materializers stream each shard continuously — chunks for history, the
   in-memory buffer for recent data — and serve reads without ever touching
   the WAL.

The [Durability Foundation guide](guides/durability-foundation.md) tells this
story end to end, including what happens on restart.

## Object storage

The local filesystem is the default backend. To point durable history at any
S3-compatible store (AWS S3, MinIO, ...):

```elixir
config :bedrock, Bedrock.ObjectStorage,
  backend: :s3,
  s3: [
    bucket: "bedrock",
    access_key_id: "...",
    secret_access_key: "...",
    region: "us-east-1"
  ]
```

See the [S3 Object Storage guide](guides/object-storage-s3.md) for
conditional-write semantics and the full option set.

## Production configuration

Bedrock validates a durability profile at startup — replication factor, log
count, and persistent paths for coordinator, log, and materializer roles. The
default mode is `:strict`, which fails fast when requirements are not met.
For local development or a single-node rollout, relax it explicitly:

```elixir
config :bedrock, MyApp.Cluster,
  durability_mode: :relaxed
```

Details in the [Durability Profile guide](guides/durability-profile.md).

## Documentation

The guides are organized in three tiers — quick reads for orientation, guides
for tasks, deep dives for internals:

- **Start here:** [User's Perspective](guides/quick-reads/users-perspective.md) ·
  [Transaction Basics](guides/quick-reads/transactions.md) ·
  [System Layout](guides/quick-reads/transaction-system-layout.md)
- **Architecture:** [Data Plane](guides/quick-reads/data-plane.md) ·
  [Control Plane](guides/quick-reads/control-plane.md) ·
  [Recovery](guides/quick-reads/recovery.md)
- **Durability:** [Durability Foundation](guides/durability-foundation.md) ·
  [Durability Profile](guides/durability-profile.md) ·
  [S3 Backend](guides/object-storage-s3.md) ·
  [Async Persistence Queue](guides/async-persistence-queue.md)
- **Deep dives:** [Architecture](guides/deep-dives/architecture.md) ·
  [Transactions](guides/deep-dives/transactions.md) ·
  [Recovery](guides/deep-dives/recovery.md) ·
  [Cluster Startup](guides/deep-dives/cluster-startup.md)

The full index lives in [guides/ai-start-here.md](guides/ai-start-here.md) and
the [glossary](guides/glossary.md).

## Status

Bedrock is pre-1.0 and under active development. The transaction system, the
durability pipeline, and the S3 backend are exercised by unit, MinIO-backed
integration, and distributed durability test suites — but the API may still
shift between minor versions. We'd love early feedback.

## Development and testing

```bash
mix test                    # default suite
mix test --include s3       # + MinIO-backed S3 integration tests
```

S3-tagged tests need a local MinIO binary
(`MIX_ENV=test mix minio_server.download --arch darwin-arm64 --version latest`)
and are skipped automatically when it isn't available. The distributed
durability suite is documented in
[guides/distributed-durability-tests.md](guides/distributed-durability-tests.md).

## License

Bedrock is released under the [MIT License](LICENSE).
