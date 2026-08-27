# Bedrock Glossary

This glossary defines key terms and concepts used throughout the Bedrock distributed key-value store documentation and codebase.

## Quick Navigation

**[A](#a) • [B](#b) • [C](#c) • [D](#d) • [E](#e) • [F](#f) • [G](#g) • [H](#h) • [K](#k) • [L](#l) • [M](#m) • [O](#o) • [P](#p) • [R](#r) • [S](#s) • [T](#t) • [V](#v) • [W](#w)**

---

## A

### **Available After**

The exclusive WAL cursor after which every retained transaction is available.
Shale persists it as the `previous_version` in each segment header, so trimming
and cold restart cannot erase the boundary needed by `Log.pull/3`. It is not the
same as the first retained transaction.

### **ACID**

**Atomicity, Consistency, Isolation, Durability** - The four fundamental properties of database transactions that Bedrock guarantees. All operations in a transaction either succeed together (atomicity), maintain data validity (consistency), appear isolated from other transactions (isolation), and survive system failures (durability).

---

## B

### **Batch**

A group of transactions processed together by a Commit Proxy for efficiency. Batching amortizes the cost of conflict resolution and logging across multiple transactions.

### **Batching**

The strategy of processing multiple transactions together to amortize overhead costs and improve throughput while managing latency.

---

## C

### **Chunk**

An immutable object-storage file holding one shard's transaction slices for a range of versions. Chunks are written by ShardServers on Demux-commanded cuts and are named for the last commit they contain, so a reader can find the chunk covering any version with a single next-key-after listing call. Because cuts are deterministic and gated on the known committed version, every replica produces byte-identical chunks.

### **Currency**

The high-water knowledge carried on every ShardServer pull reply: "here is your data" or "nothing for you, but you are current through version v." Busy shards learn it from slice pushes; idle shards learn it by subscription — a parked materializer asks its Demux once, and the Demux answers the moment its high-water advances. Currency is what lets a materializer for a quiet shard keep advancing its version (and serving fresh reads) without any timers or polling.

### **Cut**

A deterministic version-time boundary at which the Demux commands every ShardServer to persist its buffered slices as a chunk. Cuts are pure version arithmetic (fixed buckets of the cut interval) and fire only once the known committed version has reached them, so nothing that is not known-committed ever becomes durable in object storage. The WAL's active segment rolls on the same boundaries, so trimming can physically drop history at the cut cadence.

### **Cold Start**

The process of starting a Bedrock cluster from scratch, involving coordinator election, director startup, service discovery, and range assignment.

### **Commit**

The process of making a transaction's changes permanent and visible to other transactions. In Bedrock, this involves conflict resolution, version assignment, and durable logging.

### **Commit Proxy**

The component responsible for batching transactions, coordinating conflict resolution, and ensuring durable persistence through log servers. See also: [Commit Proxy implementation](deep-dives/architecture/data-plane/commit-proxy.md).

### **Commit Version**

The globally unique version number assigned to a transaction when it commits, determining its position in the global transaction order. Forms part of the Lamport clock chain with the last commit version.

### **Conflict**

A situation where transactions interfere with each other in ways that would violate isolation. Types include read-write conflicts, write-write conflicts, and within-batch conflicts.

### **Control Plane**

The management layer consisting of Coordinators and Directors that handle cluster coordination, recovery, and system configuration.

---

## D

### **Data Plane**

The transaction processing layer consisting of Sequencers, Commit Proxies, Resolvers, Logs, and Materializers that handle client transactions.

### **Demux**

The process tree owned by each running log that slices every pushed transaction by shard and routes the slices to anonymous, replica-local ShardServers. Its shard map is the only registry for those children. The Demux commands deterministic chunk cuts, tracks that log replica's minimum durable version that gates WAL trimming, and answers currency subscriptions so idle shards' materializers stay current without polling. It is the log's only data-plane consumer, and materializers' only data-plane source.

### **Director**

The control plane component responsible for recovery coordination, health monitoring, and data plane component management. See also: [Director implementation](deep-dives/architecture/control-plane/director.md).

### **Distributed Key-Value Store**

A database system that stores data as key-value pairs across multiple machines, providing scalability and fault tolerance. Bedrock implements this with strong consistency guarantees.

### **Durability Guarantee**

The promise that once a transaction is committed and acknowledged, it will survive system failures and be permanently stored. In Bedrock, log acknowledgment means WAL append + fsync has completed on required log replicas; async object persistence may still be catching up.

---

## E

### **Encoding**

A module responsible for packing and unpacking keys or values, attached to a
`Bedrock.Keyspace` rather than to the repo. `Bedrock.Encoding.Tuple` implements
the FoundationDB tuple layer, `Bedrock.Encoding.None` passes bytes through,
and `Bedrock.Encoding.BERT` serializes Elixir terms. The repo API itself is
binary in, binary out.

### **Epoch**

A recovery generation number that increases with each cluster recovery, used to reject stale requests from previous recovery attempts.

### **Eventually Consistent**

The property that materializers will eventually reflect all committed transactions, though there may be temporary delays in applying changes.

---

## F

### **Fail-Fast Recovery**

Bedrock's recovery philosophy where components exit immediately on unrecoverable errors, triggering director-coordinated recovery rather than attempting complex error handling.

### **Finalization Pipeline**

The 8-step process that Commit Proxies use to process transaction batches: create plan, prepare for resolution, resolve conflicts, handle aborts, prepare for logging, push to logs, notify sequencer, notify successes.

### **FoundationDB**

The distributed database architecture that Bedrock follows, separating control plane (coordination & recovery) from data plane (transaction processing) with specialized components for different aspects of transaction processing. The foundational research includes the [primary SIGMOD 2021 paper](https://dl.acm.org/doi/10.1145/3448016.3457559) on FoundationDB's unbundled architecture, the [SIGMOD 2019 paper](https://dl.acm.org/doi/10.1145/3299869.3314039) on the Record Layer, and the [VLDB 2018 paper](http://www.vldb.org/pvldb/vol11/p540-shraer.pdf) on CloudKit's use at scale. Learn more at [FoundationDB.org](https://www.foundationdb.org/).

---

## G

### **Generation**

One recovery's set of logs. Recovery does not repair old logs in place: it recruits a fresh generation, replays the surviving WAL tail into it, and lets [reconciliation](#reconciliation) retire the previous generation once the new layout is durable. Because the old WAL was trimmed in normal operation, the replay copies a few seconds of tail rather than the cluster's history.

---

## H

### **Horse Racing**

A distributed systems performance optimization technique where multiple equivalent service endpoints are queried simultaneously, using the first successful response while canceling remaining requests. This approach automatically adapts to varying network conditions, server load, and geographic latency without requiring complex load balancing logic. See [Transaction Builder implementation](deep-dives/architecture/infrastructure/transaction-builder.md#materializer-selection-and-performance-optimization) for Bedrock's specific use of horse racing in materializer selection.

### **Hot Key**

A key that receives disproportionately high read or write traffic, potentially becoming a performance bottleneck.

---

## K

### **Key Distribution**

The process of spreading keys across multiple materializers to balance load and avoid hotspots.

### **Key Range**

A contiguous segment of the key space, defined by start and end keys (e.g., `{"a", "m"}` covers keys from "a" to just before "m"). Used for sharding data across materializers and resolvers.

### **Known Committed Version**

The highest version number confirmed as durably committed across all log servers, serving as the readable horizon for new transactions. Returned by read version requests to ensure consistent snapshots, and piggybacked by commit proxies on every log push (FoundationDB tlog parity): downstream durability — chunk cuts in the Demux and window eviction in materializers — is gated on it, so nothing that is not known-committed ever becomes durable anywhere, and recovery rollback is pure pointer manipulation.

---

## L

### **Lamport Clock**

A logical clock mechanism used by the Sequencer to assign globally ordered version numbers that preserve causality in the distributed system. Implemented as version pairs {last_commit_version, next_commit_version}.

### **Last Commit Version**

The most recent version number handed to a commit proxy by the Sequencer. Forms the Lamport clock chain with the next commit version for conflict detection.

### **Link**

The per-node client-facing interface (formerly called the Gateway). It discovers
the cluster, holds this epoch's transaction system layout, and owns the node's
routing cache — the partial, coalescing index of per-key routing answers fetched
from commit proxies, read directly from ETS on every key lookup. See also:
[Link implementation](deep-dives/architecture/infrastructure/link.md).

### **Lock Token**

A unique identifier used during recovery to ensure only authorized recovery operations can unlock components after recovery completes.

### **Log**

The component that provides durable, ordered transaction storage and serves as the authoritative record of committed transactions. See also: [Log implementation](deep-dives/architecture/data-plane/log.md).

---

## M

### **Materializer**

The component that materializes queryable, versioned key-value state for a single shard: it streams that shard's slices from a log's Demux and serves reads from the result. See [Materializer](deep-dives/architecture/data-plane/materializer.md), and [Olivine](#olivine) for the engine that implements it.

FoundationDB calls this role a *storage server*. Bedrock calls it a materializer; [Storage](#storage) notes the two places the older word still appears in the code.

A materializer is not [Object Storage](#object-storage), which holds the durable bytes its history comes from.

### **Manifest**

A configuration file that describes worker capabilities and system configuration for service discovery.

### **Minimum Read Version**

## Minimum Read Version (MRV) / Oldest Read Version

The oldest version number still needed by any active transaction. Used for garbage collection of old version history.

### **Multi-Version Concurrency Control**

## Multi-Version Concurrency Control (MVCC)

A concurrency control method that maintains multiple versions of each data item, allowing transactions to read consistent snapshots while writes proceed concurrently.

---

## O

### **Object Storage**

The durability substrate beneath the data plane: a key-value store of
binary objects, backed by the local filesystem or S3.

A log's Demux writes each shard's committed transactions here as
[chunks](#chunk), and that log's [ShardServer](#shardserver) reads them
back to serve a materializer's history. A [Materializer](#materializer)
therefore reaches object storage through a ShardServer, and never reads a
log's WAL directly. A log trims a WAL segment once object storage
confirms the transactions that segment holds.

Chunks and snapshots are written once and never modified — each is
published with a create-only put. The cluster-state object is the
exception, and is updated in place.

Object storage serves no client reads and holds no queryable state.
Serving reads is a [Materializer](#materializer)'s work.

### **Olivine**

The materializer engine implementation: a versioned page index over one shard's key range, fed by a single stream (snapshot, then chunks, then the ShardServer buffer). Applies eagerly for read currency but persists to disk only up to the known committed version, which makes recovery rollback a pure in-memory pointer discard. See also: [Olivine implementation details](deep-dives/architecture/implementations/olivine.md).

### **Optimistic Concurrency Control**

## Optimistic Concurrency Control (OCC)

A concurrency control method where transactions proceed without locking, with conflicts detected and resolved at commit time. Enables high performance but requires retry logic for conflicted transactions.

---

## P

### **Pipelining**

The performance optimization where multiple phases of transaction processing overlap to improve overall throughput.

---

## R

### **Range Tag**

An identifier for a group of key ranges that are processed together, used for efficient distribution of transactions across logs.

### **Read Version**

The version number that determines which committed state a transaction sees for all its read operations, ensuring consistent snapshots. Always returns the known committed version from the Sequencer.

### **Read-Your-Writes Consistency**

The guarantee that within a transaction, read operations immediately see the effects of previous write operations in the same transaction.

### **Recovery**

The process of restoring system state after failures, coordinated by the Director and involving state reconstruction from durable logs.

### **Reconciliation**

The single destruction path for workers, mirroring recovery as the single creation path. When a newly durable transaction system layout is broadcast, each foreman retires every worker it hosts that the layout does not reference: previous-generation logs and any strays left by interrupted recovery attempts. A worker, for this purpose, is a directory with a valid manifest; anything the foreman cannot identify is left alone.

### **Recovery Info**

State information provided by components during recovery, including version numbers, durability status, and operational state. Logs report their oldest and newest versions; materializers report their durable version and shard assignment, which is how recovery reuses survivors.

### **Resolver**

The component that implements MVCC conflict detection for specific key ranges, maintaining version history and detecting transaction conflicts. See also: [Resolver implementation](deep-dives/architecture/data-plane/resolver.md).

---

## S

### **Sequencer**

The component responsible for assigning globally unique, monotonically increasing version numbers to transactions (Lamport clock implementation). See also: [Sequencer implementation](deep-dives/architecture/data-plane/sequencer.md).

### **Service Descriptor**

A data structure that describes the current status and capabilities of a system component.

### **Shard**

An independent partition of data identified by range tags, enabling parallel processing and fault tolerance. Services with identical tag sets serve the same shard and can substitute for each other, while services with different tag sets serve different shards and cannot be interchanged.

### **Shale**

The log storage engine implementation that provides durable, append-only transaction logging with strict version ordering. This is one kind of [Log](deep-dives/architecture/data-plane/log.md) server implementation. See also: [Shale implementation details](deep-dives/architecture/implementations/shale.md).

### **ShardServer**

An anonymous per-shard process owned by exactly one log's Demux. It buffers that replica's recent transaction slices, persists them as shared deterministic chunks on commanded cuts, and serves the shard's continuous stream to materializers — chunks for history, buffer for recent data, with version currency on every reply. Replicated logs own distinct ShardServers for the same logical shard and advance their WAL trim floors only from their own child's confirmations.

### **Storage**

The earlier name for a [Materializer](#materializer). The code uses *materializer* today, with two exceptions that still carry the older word: `Bedrock.Internal.TransactionBuilder.StorageRacing`, and the `mix bedrock.dump_storage` task.

[Object Storage](#object-storage) is a separate component whose name also contains the word — the durable byte store beneath the data plane, rather than anything that serves reads.

### **Strict Serialization**

The strongest isolation level where transactions appear to execute in some sequential order, with no interleaving of operations.

### **System Keys**

Keys under the `\xff/system` prefix, which Bedrock uses for cluster metadata: `shard_keys/` for shard boundaries, `materializers/` for shard membership, and `distributor_lock/` for the Distributor's write fence. Writable only by system-mode commits. See [The System Keyspace](quick-reads/system-keyspace.md).

---

## T

### **Tag Coverage**

The mapping of which log servers are responsible for storing transactions affecting specific range tags.

### **Transaction**

A unit of work that groups multiple read and write operations together with ACID guarantees.

### **Transaction Builder**

A per-transaction process that manages the complete lifecycle of a single transaction, from read version acquisition through commit coordination. See also: [Transaction Builder implementation](deep-dives/architecture/infrastructure/transaction-builder.md).

### **Transaction System Layout**

The blueprint that defines how all components in a Bedrock cluster connect and communicate during transaction processing. Contains component process IDs, key range assignments, service mappings, and operational status for the entire cluster. Once durable, it is also the single source of truth for what should exist: [reconciliation](#reconciliation) retires any worker the layout does not reference. See also: [Transaction System Layout overview](quick-reads/transaction-system-layout.md).

### **Trim Floor**

The version below which a log's WAL may be recycled. The floor is object-storage confirmation alone — the minimum cut every shard has confirmed durable in chunks — so readers never hold the WAL back. It is deliberately not persisted: on restart it regresses to what the on-disk segments define and re-derives from fresh confirmations.

---

## V

### **Version**

A globally unique, monotonically increasing number assigned to transactions that determines their order in the system. Two types: read versions (for snapshots) and commit versions (for ordering).

### **Version Chain Integrity**

The property that each commit references the previous committed version, maintained by the Sequencer to enable proper conflict detection.

### **Version Gap**

A situation where a commit version was assigned but the transaction failed to commit, leaving a gap in the version sequence.

---

## W

### **Worker**

A generic term for any service process in the Bedrock cluster (materializers, log servers, etc.).
