# Bedrock Glossary

This glossary defines key terms and concepts used throughout the Bedrock distributed key-value store documentation and codebase.

---

<a id="acid"></a>

**ACID**
: **Atomicity, Consistency, Isolation, Durability** - The four fundamental properties of database transactions that Bedrock guarantees. All operations in a transaction either succeed together (atomicity), maintain data validity (consistency), appear isolated from other transactions (isolation), and survive system failures (durability).

<a id="basalt"></a>

**[Basalt](components/basalt.md)** (one kind of [Storage](components/storage.md))
: A storage engine implementation that provides multi-version key-value storage with MVCC support and transaction log integration.

<a id="batch"></a>

**Batch**
: A group of transactions processed together by a Commit Proxy for efficiency. Batching amortizes the cost of conflict resolution and logging across multiple transactions.

<a id="batching"></a>

**Batching**
: The strategy of processing multiple transactions together to amortize overhead costs and improve throughput while managing latency.

<a id="codec"></a>

**Codec**
: A module responsible for encoding/decoding keys or values for storage and transmission. Examples: `TupleKeyCodec`, `BertValueCodec`.

<a id="cold-start"></a>

**Cold Start**
: The process of starting a Bedrock cluster from scratch, involving coordinator election, director startup, service discovery, and range assignment.

<a id="commit"></a>

**Commit**
: The process of making a transaction's changes permanent and visible to other transactions. In Bedrock, this involves conflict resolution, version assignment, and durable logging.

<a id="commit-proxy"></a>

**[Commit Proxy](components/commit-proxy.md)**
: The component responsible for batching transactions, coordinating conflict resolution, and ensuring durable persistence through log servers.

<a id="commit-version"></a>

**Commit Version**
: The globally unique version number assigned to a transaction when it commits, determining its position in the global transaction order. Forms part of the Lamport clock chain with the last commit version.

<a id="conflict"></a>

**Conflict**
: A situation where transactions interfere with each other in ways that would violate isolation. Types include read-write conflicts, write-write conflicts, and within-batch conflicts.

<a id="control-plane"></a>

**Control Plane**
: The management layer consisting of Coordinators and Directors that handle cluster coordination, recovery, and system configuration.

<a id="data-plane"></a>

**Data Plane**
: The transaction processing layer consisting of Sequencers, Commit Proxies, Resolvers, Logs, and Storage servers that handle client transactions.

<a id="director"></a>

**[Director](components/director.md)**
: The control plane component responsible for recovery coordination, health monitoring, and data plane component management.

<a id="distributed-key-value-store"></a>

**Distributed Key-Value Store**
: A database system that stores data as key-value pairs across multiple machines, providing scalability and fault tolerance. Bedrock implements this with strong consistency guarantees.

<a id="durability-guarantee"></a>

**Durability Guarantee**
: The promise that once a transaction is committed and acknowledged, it will survive system failures and be permanently stored.

<a id="epoch"></a>

**Epoch**
: A recovery generation number that increases with each cluster recovery, used to reject stale requests from previous recovery attempts.

<a id="eventually-consistent"></a>

**Eventually Consistent**
: The property that storage servers will eventually reflect all committed transactions, though there may be temporary delays in applying changes.

<a id="fail-fast-recovery"></a>

**Fail-Fast Recovery**
: Bedrock's recovery philosophy where components exit immediately on unrecoverable errors, triggering director-coordinated recovery rather than attempting complex error handling.

<a id="finalization-pipeline"></a>

**Finalization Pipeline**
: The 8-step process that Commit Proxies use to process transaction batches: create plan, prepare for resolution, resolve conflicts, handle aborts, prepare for logging, push to logs, notify sequencer, notify successes.

<a id="foundationdb"></a>

**[FoundationDB](https://www.foundationdb.org/)**
: The architectural pattern that Bedrock follows, separating control plane (coordination & recovery) from data plane (transaction processing) with specialized components for different aspects of transaction processing. Their [paper](https://www.foundationdb.org/files/fdb-paper.pdf) is awesome. Go read it!

<a id="gateway"></a>

**[Gateway](components/gateway.md)**
: The client-facing interface that manages transaction coordination, read version leasing, and serves as the entry point for all client operations.

<a id="horse-racing"></a>

**Horse Racing**
: A performance optimization where Transaction Builders query multiple storage servers in parallel and use the first successful response.

<a id="hot-key"></a>

**Hot Key**
: A key that receives disproportionately high read or write traffic, potentially becoming a performance bottleneck.

<a id="key-distribution"></a>

**Key Distribution**
: The process of spreading keys across multiple storage servers to balance load and avoid hotspots.

<a id="key-range"></a>

**Key Range**
: A contiguous segment of the key space, defined by start and end keys (e.g., `{"a", "m"}` covers keys from "a" to just before "m"). Used for sharding data across storage servers and resolvers.

<a id="known-committed-version"></a>

**Known Committed Version**
: The highest version number confirmed as durably committed across all log servers, serving as the readable horizon for new transactions. Returned by read version requests to ensure consistent snapshots.

<a id="lamport-clock"></a>

**Lamport Clock**
: A logical clock mechanism used by the Sequencer to assign globally ordered version numbers that preserve causality in the distributed system. Implemented as version pairs {last_commit_version, next_commit_version}.

<a id="last-commit-version"></a>

**Last Commit Version**
: The most recent version number handed to a commit proxy by the Sequencer. Forms the Lamport clock chain with the next commit version for conflict detection.

<a id="lease"></a>

**Lease**
: A time-limited grant for using a read version, managed by the Gateway to ensure transactions don't use stale versions.

<a id="lease-renewal"></a>

**Lease Renewal**
: The process of extending a read version lease before it expires, enabling long-running transactions.

<a id="lock-token"></a>

**Lock Token**
: A unique identifier used during recovery to ensure only authorized recovery operations can unlock components after recovery completes.

<a id="log"></a>

**[Log](components/log.md)**
: The component that provides durable, ordered transaction storage and serves as the authoritative record of committed transactions.

<a id="manifest"></a>

**Manifest**
: A configuration file that describes worker capabilities and system configuration for service discovery.

<a id="minimum-read-version"></a>

**Minimum Read Version (MRV)**
: The oldest version number still needed by any active transaction. Used for garbage collection of old version history.

<a id="multi-version-concurrency-control-mvcc"></a>

**Multi-Version Concurrency Control (MVCC)**
: A concurrency control method that maintains multiple versions of each data item, allowing transactions to read consistent snapshots while writes proceed concurrently.

<a id="optimistic-concurrency-control-occ"></a>

**Optimistic Concurrency Control (OCC)**
: A concurrency control method where transactions proceed without locking, with conflicts detected and resolved at commit time. Enables high performance but requires retry logic for conflicted transactions.

<a id="pipelining"></a>

**Pipelining**
: The performance optimization where multiple phases of transaction processing overlap to improve overall throughput.

<a id="range-tag"></a>

**Range Tag**
: An identifier for a group of key ranges that are processed together, used for efficient distribution of transactions across logs.

<a id="read-version"></a>

**Read Version**
: The version number that determines which committed state a transaction sees for all its read operations, ensuring consistent snapshots. Always returns the known committed version from the Sequencer.

<a id="read-your-writes-consistency"></a>

**Read-Your-Writes Consistency**
: The guarantee that within a transaction, read operations immediately see the effects of previous write operations in the same transaction.

<a id="recovery"></a>

**Recovery**
: The process of restoring system state after failures, coordinated by the Director and involving state reconstruction from durable logs.

<a id="recovery-info"></a>

**Recovery Info**
: State information provided by components during recovery, including version numbers, durability status, and operational state.

<a id="resolver"></a>

**[Resolver](components/resolver.md)**
: The component that implements MVCC conflict detection for specific key ranges, maintaining version history and detecting transaction conflicts.

<a id="sequencer"></a>

**[Sequencer](components/sequencer.md)**
: The component responsible for assigning globally unique, monotonically increasing version numbers to transactions (Lamport clock implementation).

<a id="service-descriptor"></a>

**Service Descriptor**
: A data structure that describes the current status and capabilities of a system component.

<a id="split-brain"></a>

**Split-Brain**
: A failure scenario where multiple instances of the same system component operate simultaneously, potentially creating conflicting states. In Bedrock, epoch-based locking prevents split-brain by ensuring only one director can control services at a time.

<a id="shale"></a>

**[Shale](components/shale.md)** (one kind of [Log](components/log.md))
: The log storage engine implementation that provides durable, append-only transaction logging with strict version ordering.

<a id="storage"></a>

**[Storage](components/storage.md)**
: The component that serves read requests and maintains versioned key-value data by pulling committed transactions from logs.

<a id="storage-team"></a>

**Storage Team**
: A group of storage servers that collectively handle a set of key ranges, providing replication and load distribution.

<a id="strict-serialization"></a>

**Strict Serialization**
: The strongest isolation level where transactions appear to execute in some sequential order, with no interleaving of operations.

<a id="system-keys"></a>

**System Keys**
: Special keys used internally by Bedrock for storing system configuration and metadata (e.g., `\xff/system/transaction_system_layout`).

<a id="tag-coverage"></a>

**Tag Coverage**
: The mapping of which log servers are responsible for storing transactions affecting specific range tags.

<a id="transaction"></a>

**Transaction**
: A unit of work that groups multiple read and write operations together with ACID guarantees.

<a id="transaction-builder"></a>

**[Transaction Builder](components/transaction-builder.md)**
: A per-transaction process that manages the complete lifecycle of a single transaction, from read version acquisition through commit coordination.

<a id="transaction-system-layout"></a>

**[Transaction System Layout](transaction-system-layout.md)**
: The blueprint that defines how all components in a Bedrock cluster connect and communicate during transaction processing. Contains component process IDs, key range assignments, service mappings, and operational status for the entire cluster.

<a id="version"></a>

**Version**
: A globally unique, monotonically increasing number assigned to transactions that determines their order in the system. Two types: read versions (for snapshots) and commit versions (for ordering).

<a id="version-chain-integrity"></a>

**Version Chain Integrity**
: The property that each commit references the previous committed version, maintained by the Sequencer to enable proper conflict detection.

<a id="version-gap"></a>

**Version Gap**
: A situation where a commit version was assigned but the transaction failed to commit, leaving a gap in the version sequence.

<a id="worker"></a>

**Worker**
: A generic term for any service process in the Bedrock cluster (storage servers, log servers, etc.).
