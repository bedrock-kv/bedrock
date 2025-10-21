# Bedrock Glossary

This glossary defines key terms and concepts used throughout the Bedrock distributed key-value store documentation and codebase.

## Quick Navigation

**[A](#a) • [B](#b) • [C](#c) • [D](#d) • [E](#e) • [F](#f) • [G](#g) • [H](#h) • [K](#k) • [L](#l) • [M](#m) • [O](#o) • [P](#p) • [R](#r) • [S](#s) • [T](#t) • [V](#v) • [W](#w)**

---

## A

### **ACID**

**Atomicity, Consistency, Isolation, Durability** - The four fundamental properties of database transactions that Bedrock guarantees. All operations in a transaction either succeed together (atomicity), maintain data validity (consistency), appear isolated from other transactions (isolation), and survive system failures (durability).

---

## B

### **Basalt**

A storage engine implementation that provides multi-version key-value storage with MVCC support and transaction log integration. This is one kind of [Storage](deep-dives/architecture/data-plane/storage.md) server implementation. See also: [Basalt implementation details](deep-dives/architecture/implementations/basalt.md).

### **Batch**

A group of transactions processed together by a Commit Proxy for efficiency. Batching amortizes the cost of conflict resolution and logging across multiple transactions.

### **Batching**

The strategy of processing multiple transactions together to amortize overhead costs and improve throughput while managing latency.

---

## C

### **Codec**

A module responsible for encoding/decoding keys or values for storage and transmission. Examples: `TupleKeyCodec`, `BertValueCodec`.

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

The transaction processing layer consisting of Sequencers, Commit Proxies, Resolvers, Logs, and Storage servers that handle client transactions.

### **Director**

The control plane component responsible for recovery coordination, health monitoring, and data plane component management. See also: [Director implementation](deep-dives/architecture/control-plane/director.md).

### **Distributed Key-Value Store**

A database system that stores data as key-value pairs across multiple machines, providing scalability and fault tolerance. Bedrock implements this with strong consistency guarantees.

### **Durability Guarantee**

The promise that once a transaction is committed and acknowledged, it will survive system failures and be permanently stored.

---

## E

### **Epoch**

A recovery generation number that increases with each cluster recovery, used to reject stale requests from previous recovery attempts.

### **Eventually Consistent**

The property that storage servers will eventually reflect all committed transactions, though there may be temporary delays in applying changes.

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

### **Gateway**

The client-facing interface that manages transaction coordination and serves as the entry point for all client operations. See also: [Gateway implementation](deep-dives/architecture/infrastructure/gateway.md).

---

## H

### **Horse Racing**

A distributed systems performance optimization technique where multiple equivalent service endpoints are queried simultaneously, using the first successful response while canceling remaining requests. This approach automatically adapts to varying network conditions, server load, and geographic latency without requiring complex load balancing logic. See [Transaction Builder implementation](deep-dives/architecture/infrastructure/transaction-builder.md#storage-server-selection-and-performance-optimization) for Bedrock's specific use of horse racing in storage server selection.

### **Hot Key**

A key that receives disproportionately high read or write traffic, potentially becoming a performance bottleneck.

---

## K

### **Key Distribution**

The process of spreading keys across multiple storage servers to balance load and avoid hotspots.

### **Key Range**

A contiguous segment of the key space, defined by start and end keys (e.g., `{"a", "m"}` covers keys from "a" to just before "m"). Used for sharding data across storage servers and resolvers.

### **Known Committed Version**

The highest version number confirmed as durably committed across all log servers, serving as the readable horizon for new transactions. Returned by read version requests to ensure consistent snapshots.

---

## L

### **Lamport Clock**

A logical clock mechanism used by the Sequencer to assign globally ordered version numbers that preserve causality in the distributed system. Implemented as version pairs {last_commit_version, next_commit_version}.

### **Last Commit Version**

The most recent version number handed to a commit proxy by the Sequencer. Forms the Lamport clock chain with the next commit version for conflict detection.

### **Lock Token**

A unique identifier used during recovery to ensure only authorized recovery operations can unlock components after recovery completes.

### **Log**

The component that provides durable, ordered transaction storage and serves as the authoritative record of committed transactions. See also: [Log implementation](deep-dives/architecture/data-plane/log.md).

---

## M

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

### **Recovery Info**

State information provided by components during recovery, including version numbers, durability status, and operational state.

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

### **Storage**

The component that serves read requests and maintains versioned key-value data by pulling committed transactions from logs. See also: [Storage implementation](deep-dives/architecture/data-plane/storage.md).

### **Storage Team**

A group of storage servers that collectively handle a set of key ranges, providing replication and load distribution.

### **Strict Serialization**

The strongest isolation level where transactions appear to execute in some sequential order, with no interleaving of operations.

### **System Keys**

Special keys used internally by Bedrock for storing system configuration and metadata (e.g., `\xff/system/transaction_system_layout`).

---

## T

### **Tag Coverage**

The mapping of which log servers are responsible for storing transactions affecting specific range tags.

### **Transaction**

A unit of work that groups multiple read and write operations together with ACID guarantees.

### **Transaction Builder**

A per-transaction process that manages the complete lifecycle of a single transaction, from read version acquisition through commit coordination. See also: [Transaction Builder implementation](deep-dives/architecture/infrastructure/transaction-builder.md).

### **Transaction System Layout**

The blueprint that defines how all components in a Bedrock cluster connect and communicate during transaction processing. Contains component process IDs, key range assignments, service mappings, and operational status for the entire cluster. See also: [Transaction System Layout overview](quick-reads/transaction-system-layout.md).

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

A generic term for any service process in the Bedrock cluster (storage servers, log servers, etc.).
