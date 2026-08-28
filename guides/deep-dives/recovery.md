# Bedrock's Recovery Architecture: Building Reliability Through Simplicity

When distributed systems fail, engineers face a fundamental choice: patch the immediate problems or rebuild from a trusted foundation. Bedrock chooses reconstruction over repair, rebuilding the entire [transaction processing infrastructure](architecture.md) from verified persistent state when any critical component fails. This approach trades recovery time—typically well under a second—for absolute confidence in system integrity.

Bedrock inverts traditional distributed systems priorities, following FoundationDB's architectural principle[^1]: optimize for the worst-case scenario rather than normal operation. When anything goes wrong, assume everything might be compromised and rebuild the entire transaction system. This philosophy rests on three key insights: reliability theory favors simplicity over complex recovery optimization, comprehensive reconstruction enables complete confidence while targeted repairs leave lingering questions about hidden corruption, and complex recovery logic is fundamentally untestable across every possible failure combination. Simple, comprehensive recovery provides a single, well-defined path that can be thoroughly validated against real-world scenarios.

Two rules keep that path simple:

**An attempt is a pure function of the current view.** Recovery runs against whatever the coordinator's service directory says right now, and a changed view retriggers it. An attempt that fires before the workers it needs have registered fails in microseconds and mutates nothing; the registration that changes the view triggers the attempt that succeeds. There are no waiting heuristics, no grace periods, and no give-up timers — the event-driven retry is the wait mechanism. A typical restart converges in two attempts: one premature, one complete.

**The durable layout is the single source of truth for what exists.** Recovery is the only path that creates workers. When a recovery completes, every foreman relays the durable layout to the workers it hosts, and each worker decides its own retirement — a log checks the layout's epoch-constant log set; a materializer asks a commit proxy whether the committed `materializers/<tag>` entry still names it (FDB-style: no component decides another process's retirement; the foreman only janitors what a retiring worker leaves behind). Old-generation logs and any strays from interrupted attempts self-dispose by the same rule — no stray detection, no age heuristics, no cleanup jobs. Between the two rules, debris is impossible: attempts are free, and anything an attempt leaves behind self-retires when a later attempt succeeds.

Recovery proceeds through a sequence of phases, each building on verified results from the previous one.

## Phase 0: [TSL Validation](../quick-reads/recovery/tsl-validation.md)

Recovery's first line of defense: the recovered Transaction System Layout is checked for type safety and sanity before anything trusts it. If the persisted configuration is corrupt, recovery stops here rather than building on it.

## Phase 1: [Service Locking](../quick-reads/recovery/service-locking.md)

Recovery establishes exclusive control by locking the old layout's logs and every advertised materializer. Locking does two jobs at once. It fences: a locked service reports only to this recovery's director, and an older epoch's director can never reclaim it. And it informs: each locked service returns its recovery info — logs report the persisted exclusive `available_after` cursor, their first retained transaction for inspection, and their inclusive endpoint; materializers report their durable version and shard assignment. That information is what lets the later phases reuse survivors instead of discarding them.

## Phase 2: [Log Recovery Planning](../quick-reads/recovery/log-recovery-planning.md)

From the locked logs, recovery computes the version vector `{available_after, last_inclusive}`: the common range `(available_after, last_inclusive]` guaranteed complete across the surviving majority. The first element is a cursor persisted in every WAL segment header, not the first transaction. The second is the recovery endpoint. This phase also seeds vacancies for a fresh generation of logs: Bedrock does not repair old logs in place, it replaces them and copies the data forward.

## Phase 3: [Log Recruitment](../quick-reads/recovery/log-recruitment.md)

The log vacancies are filled: available log workers are reused as candidates, and new workers are created where candidates run out. A worker created during the attempt cannot yet appear in the coordinator's directory — advertisement is asynchronous — so recruitment locks its own creations through the references it already holds rather than waiting to rediscover them. Recruitment therefore completes in the same attempt that started it.

## Phase 4: [Log Replay](../quick-reads/recovery/log-replay.md)

Committed transactions are copied from the surviving logs into the new generation. Object storage is durable through `durable_through`, so replay uses the exclusive cursor `replay_after = max(durable_through, available_after)` and copies `(replay_after, last_inclusive]`. It routes every copied transaction through the new log's fresh Demux, so the per-shard buffers and chunk pipeline are rebuilt as a side effect — deterministic cuts reproduce byte-identical chunks, and "already exists" is a truthful confirmation. No lower-bound sentinel is appended: an empty range persists only its cursor, while a non-empty replay succeeds only after observing the inclusive endpoint. Because the old WAL was trimmed in normal operation, this copies a few seconds of tail, not the cluster's lifetime; replay cost is independent of cluster age.

## Phase 5: [Sequencer Startup](../quick-reads/recovery/sequencer-startup.md)

The singleton version authority starts at the recovery version. From here on, every version it mints is newer than anything the old system produced, and the known committed version it tracks gates all downstream durability.

## Phase 6: Materializer Bootstrap

The surviving materializers get their shards back. Recovery indexes the locked materializers by shard assignment (when several claim one shard — strays from an interrupted attempt — the most-advanced durable state wins), unlocks each with its shard's logs at the recovery version, and lets it resume streaming from its own applied position. Unlocking at the recovery version means an in-memory rollback at most: a materializer's disk never holds anything above the known committed version, so there is never disk surgery.

The system-shard materializer has one extra job: it holds the shard layout — the map from key ranges to shards — in ordinary system keys. Recovery waits for it to catch up to the recovery version by streaming the replayed WAL from the new log's Demux, then reads the layout from it. That layout drives everything placed afterward: which shards need materializers, and where resolvers split the keyspace. Only a shard with no survivor gets a freshly created materializer, which starts empty and rebuilds from object-storage chunks.

## Phase 7: [Commit Proxy Startup](../quick-reads/recovery/proxy-startup.md)

Commit proxies are deployed across coordination-capable nodes and held locked until the layout phase releases them, preventing premature transaction processing.

## Phase 8: [Resolver Startup](../quick-reads/recovery/resolver-startup.md)

MVCC conflict detection starts fresh each epoch: one resolver per shard range in the recovered layout. Resolvers are pure derived state, so recreating them is cheap and leaves no continuity questions.

## Phase 9: [Transaction System Layout](../quick-reads/recovery/transaction-system-layout.md)

Recovery assembles the coordination blueprint: the new-generation logs, the active materializers for every shard, the proxies, the resolvers, and the shard layout. The services named here are a complete statement of what should exist — which is exactly what worker reconciliation will enforce once the layout is durable. Workers created during this very attempt are included from recovery's own records, since they may not have advertised yet.

## Phase 10: [Monitoring](../quick-reads/recovery/monitoring.md)

Every component is monitored before the final system transaction, so a failure during that transaction triggers fail-fast director shutdown and a fresh recovery rather than a wedged repair loop.

## Phase 11: [Persistence](../quick-reads/recovery/persistence.md)

The new layout is durably stored through a system transaction — which doubles as an end-to-end proof that the freshly built pipeline can commit. Once it succeeds, the director transitions to normal operation and the coordinator broadcasts the new layout.

## After Recovery: Worker Reconciliation

The broadcast is the trigger for the second rule. Each node's Link forwards the durable layout to its foreman, and the foreman retires every worker it hosts that the layout does not reference: the previous generation's logs, whose data was replayed forward before the layout became durable, and any strays from interrupted attempts. Retirement stops the worker, deletes its directory, and removes its directory registration. A worker, for this purpose, is a directory with a valid manifest — shared-path deployments put the coordinator's raft state and the object storage root beside worker directories, and things the foreman cannot identify are not its to destroy.

## From Crisis to Confidence

What began as a system in crisis has been systematically rebuilt from verified foundations. Every component was validated, every recoverable transaction preserved through the conservative version vector, and every coordination relationship established cleanly rather than patched. The old infrastructure is gone — not lingering half-trusted, but replayed forward and reconciled away — and the new system's first committed transaction was the one that stored its own configuration.

This is the fundamental value proposition: trading well under a second of downtime for a system built entirely from verified components, whose worker population and disk footprint stay constant no matter how many times it restarts.

[^1]: FoundationDB's recovery approach is detailed in ["FoundationDB: A Distributed Unbundled Transactional Key Value Store"](https://www.foundationdb.org/files/fdb-paper.pdf) and implemented in Bedrock at `lib/bedrock/control_plane/director/recovery.ex`
