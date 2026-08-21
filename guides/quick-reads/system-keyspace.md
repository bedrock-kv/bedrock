# The System Keyspace

Everything under `\xFF/system` is Bedrock's system metadata: the facts the
cluster publishes about itself, moved the same way user data moves — as
committed, transactional key-value writes. The keyspace is the *only*
channel for this information. There are no metadata broadcasts, no shared
tables, no side files; a component that needs a system fact either reads
it from the keyspace, observes it as mutations commit, or receives a
projection of it from a component that does.

This mirrors FoundationDB's design (`\xff` system keys, `keyServers/`,
`serverList/`), and the guarantees come from the same place: because
metadata rides ordinary commits, it is ordered with the data it describes,
durable when the data is durable, and atomic with the change that makes it
true.

## Families

All keys live under the `\xff/system` prefix. Values use FDB tuple
encoding via `Bedrock.SystemKeys.Values`: encoders serve trusted writers
and raise on invalid input; decoders handle durable bytes, never raise,
and never create atoms.

### `shard_keys/<end_key>` → `{tag, start_key}`

The shard boundary map. Keys are each shard's *exclusive* end key, so
resolving a key to its shard is a ceiling search. The value carries the
shard's tag and its start key explicitly — readers consume the carried
start key rather than reconstructing it from adjacency.

Readers: every commit proxy's routing view (`RoutingData`, fed through
resolver metadata windows), the next recovery's materializer bootstrap
(the cross-epoch read that rebuilds the layout), and — indirectly —
every client, via proxy-served routing.

### `materializers/<tag>` → `{worker_id, node}`

Which materializer serves each shard: Bedrock's `serverList/` analogue,
with interfaces riding the keyspace. Both fields are strings — decoding
durable bytes never creates atoms — and consumers derive the callable
`{otp_name, node}` ref (worker OTP names are deterministic in the worker
id, so a restart on the same node changes nothing).

Readers: `RoutingData` → the client routing projection, and worker
rejoin validation — a worker (or the proxy answering for the committed
state) checks whether the entry for its tag still names it; absence means
retire.

### `layout/logs/<log_id>` → tag list

The epoch's log set. Log topology is epoch-constant — as in FoundationDB,
changing it *is* a recovery — so runtime log wiring rides the recovery
unlock seed, not mid-epoch mutations of this family. The keys are kept
for other consumers and cluster-introspection tools: a durable, queryable
statement of which logs the current epoch runs.

## Who writes, and the ownership handoff

Today every family is written by recovery's **persistence phase** in one
system transaction per epoch: each rewritten family is range-cleared and
rewritten atomically, so shrinking layouts leave no ghosts and readers
never observe a gap. The transaction commits in `:system` mode — user
commits are bounded below `\xFF` (`Bedrock.end_of_user_keyspace/0`);
system commits below `\xFF\xFF` (FDB's `ACCESS_SYSTEM_KEYS` trust model).

The per-epoch rewrite is deliberate scaffolding. In FoundationDB the Data
Distributor owns `keyServers/`/`serverKeys/` — it writes every
assignment, split, and move transactionally, and recovery never rewrites
the mapping, only re-reads it. Bedrock's Distributor (bedrock-q67.21)
takes over the same ownership: shard and materializer entries become
durable across epochs, mutated mid-epoch by ordinary transactions, and
recovery's job shrinks to re-reading and healing.

## How it moves

A system-key mutation committed through any proxy flows:

1. **Ingress** — the commit pipeline validates each transaction against
   its mode's keyspace bound; rejected transactions are replaced with
   empty ones so their conflicts never pollute resolver history.
2. **Resolvers** — every resolver records every transaction's system-key
   mutations with its local verdict, and serves each proxy an exact,
   tiling metadata window (`(last_served, last_version]`).
3. **Proxies** — windows merge (identical bounds required), verdicts AND
   positionally, vetoed mutations drop, and the survivors apply into the
   immutable `RoutingData` under a serialized apply-then-route gate — so
   a batch routes with its own committed metadata already applied.
4. **Clients** — resolve routing through a proxy (FDB's
   `GetKeyServerLocations`), cache until failure, and treat locations as
   hints: staleness costs a retry, never a wrong answer.

At recovery, proxies are seeded through `recover_from` with a routing
snapshot *derived from the keyspace* (the previous epoch's bootstrap
reads) plus runtime wiring — Bedrock's `TxnStateRequest` analogue. The
seed cannot disagree with the keyspace because it is computed from it.

## Returning families

Families exist only alongside their consumers; these return with theirs:

- **Cluster configuration** (bedrock-q67.25): parameters and policies,
  read back at recovery, changed by ordinary transactions.
- **Distributor coverage** (bedrock-q67.21): mid-epoch shard and
  materializer mutations from the coverage owner.
- **Shard genealogy** (Phase C): split/merge lineage.

See `Bedrock.SystemKeys` and `Bedrock.SystemKeys.Values` for the
authoritative key builders and codecs.
