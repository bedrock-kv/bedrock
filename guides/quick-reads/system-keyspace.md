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

### `materializers/<tag>/<worker_id>` → node

Which materializers serve each shard: Bedrock's `serverList/` analogue,
with interfaces riding the keyspace. Membership is a **set**, expressed
by key presence — the tag is major, the member id is *in* the key, and
removal is a clear of that one key. One family answers both questions
FDB needs two for: a prefix scan over a tag gives the shard's members
(FDB's range-major `keyServers/<range>` → team), and each member is
individually addressable for removal (FDB's server-major
`serverKeys/<server>/<range>`). The value is the node, as a string —
decoding durable bytes never creates atoms — and consumers derive the
callable `{otp_name, node}` ref (worker OTP names are deterministic in
the worker id, so a restart on the same node changes nothing).

Clusters written before bedrock-q67.21.9 hold single-valued
`materializers/<tag>` → `{worker_id, node}` entries. Those keys are still
recognized (`legacy_materializer_key/1`); recovery rewrites each into the
set-valued shape and clears the legacy key in the same transaction.

Readers: `RoutingData` → per-key covering entries served to clients
(`fetch_routing`), materializer rejoin validation — a worker (through the
proxy answering for the committed state) checks whether its tag's member
set still names it; absence means retire — and recovery's materializer
bootstrap, which reads the family as its re-adoption input and as the
persistence phase's diff base.

### `distributor_lock/owner`, `distributor_lock/write` → opaque UIDs

The Distributor's write fence: a port of FDB's MoveKeys lock
(`moveKeysLockOwnerKey` / `moveKeysLockWriteKey`). Every mutating
Distributor transaction reads both keys and proves inside its own
serializable commit that no newer owner has appeared, so a superseded
Distributor's writes are refused by the commit pipeline itself rather
than by cooperation. A read-only poll of the same keys is what lets an
idle zombie exit promptly.

Reader and writer: the Distributor alone
(`Bedrock.ControlPlane.Distributor.Lock`, driven by
`Distributor.Transactions`).

## Who writes, and the ownership handoff

Recovery's **persistence phase** commits one system transaction per
epoch (`:system` mode — user commits are bounded below `\xFF`,
`Bedrock.end_of_user_keyspace/0`; system commits below `\xFF\xFF`,
FDB's `ACCESS_SYSTEM_KEYS` trust model), and follows FDB's rule that
recovery never rewrites the mapping (bedrock-q67.21.2):

- `shard_keys/` — durable across epochs. Seeded only when this recovery
  invented the layout (fresh cluster, FDB's `seedShardServers`
  analogue); an existing cluster's layout is read back, and boundaries
  never change without splits, so nothing is written.
- `materializers/` — durable across epochs. Recovery reads the family
  (it is bootstrap's re-adoption authority: a family-named worker that
  this epoch locked, and whose own shard assignment agrees, beats the
  most-advanced-durable contest — which stays the fallback, including
  for tag 0, chosen before the family can be read) and writes exactly
  the assignments it changed. Entries recovery didn't touch — including strays for tags
  outside the layout — are not recovery's to clean.

The **Distributor** (bedrock-q67.21) owns `materializers/` between
recoveries. Under the `distributor_lock/` fence it publishes the
placeholder for uncovered tags, seats a real materializer when demand
arrives, and clears the entry of one that died or was parked for
idleness — all as ordinary system-mode commits, so mid-epoch membership
changes are visible to routing the moment they commit. It reads
`shard_keys/` and never writes it. Recovery's remaining share of the
family is the tag-0 metadata shard and the legacy-key migration.

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
- **Shard genealogy** (Phase C): split/merge lineage.

See `Bedrock.SystemKeys` and `Bedrock.SystemKeys.Values` for the
authoritative key builders and codecs.
