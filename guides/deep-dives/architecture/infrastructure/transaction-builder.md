# Transaction Builder

The [Transaction Builder](../../../glossary.md#transaction-builder) manages the complete lifecycle of individual [transactions](../../../glossary.md#transaction), acting as each client's dedicated transaction coordinator. Every transaction gets its own Transaction Builder process, which handles everything from [read version](../../../glossary.md#read-version) acquisition to final [commit](../../../glossary.md#commit) coordination while maintaining [read-your-writes consistency](../../../glossary.md#read-your-writes-consistency) and optimizing performance through intelligent [storage](../../../glossary.md#storage) server selection.

**Location**: [`lib/bedrock/cluster/gateway/transaction_builder.ex`](../../../lib/bedrock/cluster/gateway/transaction_builder.ex)

## Embedded Distributed Architecture Role

Transaction Builder exemplifies Bedrock's embedded distributed approach by bringing sophisticated transaction coordination directly into application processes. Rather than requiring applications to coordinate with remote transaction managers, each transaction gets its own embedded coordinator that operates within application boundaries while participating in distributed protocols.

### Local-First Transaction Coordination

The Transaction Builder represents a fundamental departure from traditional distributed database architectures. Instead of applications sending transaction requests to remote database servers, the Transaction Builder embeds transaction management logic directly within application processes. This local-first approach means transaction state, caching, and coordination logic run co-located with application code.

This embedded design enables transaction performance characteristics impossible in client-server architectures. [Read-your-writes consistency](../../../glossary.md#read-your-writes-consistency) requires no network round-trips because the write cache operates in local memory. Transaction state management happens at memory speeds rather than network speeds. Performance optimizations like [storage server](../../../glossary.md#storage-server) selection and caching develop organically within each transaction's local context.

The per-process model also enables sophisticated local optimizations. Each Transaction Builder can maintain its own performance characteristics, learning which storage servers respond fastest for its particular access patterns. This localized learning creates transaction coordination that adapts to application-specific usage patterns rather than relying on global optimization heuristics.

### Unified Failure Domain Benefits

Transaction Builder leverages one of embedded distributed systems' key advantages: simplified failure scenarios. In traditional client-server databases, applications must handle complex failure modes—what happens when the application is healthy but cannot reach the transaction coordinator? Or when network partitions isolate active transactions?

The embedded Transaction Builder eliminates these scenarios entirely. Application failure and transaction coordinator failure become the same event, creating unified failure domains that dramatically simplify both application error handling and distributed system recovery logic. When an application process fails, all its Transaction Builders fail with it, ensuring clean transaction cleanup without orphaned resources or ambiguous transaction states.

This unified approach also enables more sophisticated transaction semantics. Nested transactions can be implemented as pure local operations because they share the same failure domain as their parent transaction. Complex transaction state management becomes simple process-local operations rather than distributed coordination problems.

### Embedded Integration Advantages

The Transaction Builder's embedded architecture enables capabilities unique to this design approach. Because Transaction Builders share memory space with applications, they can implement zero-copy data structures for read and write sets, eliminating serialization overhead that dominates remote transaction systems. They can also provide application-aware optimizations, pre-loading data based on observed application patterns or batching operations for efficiency.

The embedded approach transforms operational characteristics as well. Transaction coordination capabilities are always available when applications start—there's no separate transaction manager to connect to or coordinate with. Applications and their transaction management deploy together as single units, eliminating version skew and dependency coordination issues that plague client-server architectures.

This design also enables new programming paradigms. Applications can create hundreds or thousands of concurrent transactions with minimal overhead because Transaction Builders are lightweight local processes rather than remote resources. Complex workflows with fine-grained transactional boundaries become practical in ways that would be prohibitively expensive in client-server systems.

## Why Per-Transaction Processes?

Most databases handle multiple transactions within shared processes, but Bedrock takes a different approach. Each transaction gets its own dedicated process that exists for the entire transaction lifetime. This design choice enables several important capabilities that would be difficult to achieve with shared processes.

First, it provides perfect isolation between transactions. Each Transaction Builder maintains its own read and write sets, [version](../../../glossary.md#version) [leases](../../../glossary.md#lease), and performance optimizations without any risk of cross-transaction interference. Second, it enables sophisticated state management including nested transactions and complex read-your-writes semantics. Finally, it allows each transaction to develop its own performance characteristics, learning which storage servers are fastest for its particular access patterns.

The per-process model also simplifies error handling and [recovery](../../../glossary.md#recovery). If something goes wrong with one transaction, it can fail independently without affecting other transactions. The process can maintain leases, handle timeouts, and coordinate complex multi-step operations without worrying about other transactions.

## Read-Your-Writes: The Local Cache

One of the Transaction Builder's most important responsibilities is maintaining read-your-writes consistency within transactions. When a transaction writes to a key and then immediately reads it back, it must see the value it just wrote, even though that write hasn't been committed yet.

Transaction Builder solves this by maintaining a local cache of all writes made within the transaction. When a read operation occurs, it first checks this local write cache before going to storage servers. This ensures that writes are immediately visible to subsequent reads within the same transaction, maintaining the illusion that the transaction's changes are immediately applied.

This local caching also provides significant performance benefits. Repeated reads of the same key within a transaction only hit the network once, with subsequent reads served from the local cache. For workloads that read and modify the same keys multiple times, this can dramatically reduce network overhead.

## Storage Server Selection and Performance Optimization

Transaction Builder maintains knowledge about which storage servers handle which [key ranges](../../../glossary.md#key-range), enabling it to route read requests efficiently. This mapping is derived from the [transaction system layout](../../../glossary.md#transaction-system-layout) and is kept current as the cluster configuration changes.

When a read operation needs data from storage servers, Transaction Builder faces a performance challenge: which storage server should it contact? Key ranges are typically served by multiple storage servers for redundancy, but these servers might have different response times due to load, network conditions, hardware differences, or physical location—servers in different data centers or regions can have significantly different network latencies.

Transaction Builder solves this through "[horse racing](../../../glossary.md#horse-racing)"—simultaneously querying multiple storage servers that have the needed data and using the first successful response. This approach minimizes read latency by automatically adapting to current network and server conditions without requiring complex load balancing logic.

The system also learns from these races. Transaction Builder caches information about which storage servers are fastest for different key ranges, enabling it to optimize future reads by preferring servers that have performed well recently. For subsequent reads to the same key range, it will try the cached fastest server first, falling back to horse racing if that server fails or performs poorly.

This creates a feedback loop where read performance improves over the lifetime of a transaction as the Transaction Builder builds up knowledge about the current performance characteristics of different storage servers. The component also handles automatic fallback and retry logic that keeps transactions running smoothly even when individual storage servers have problems.

## Version Management and Leasing

Transaction Builder uses lazy read version acquisition to minimize the [conflict](../../../glossary.md#conflict) detection window and ensure transactions see the latest committed data. Rather than acquiring a read version when the transaction begins, it waits until the first read operation to obtain a version. This optimization is crucial because the span from read version to [commit version](../../../glossary.md#commit-version) defines the window where this transaction could conflict with others—delaying the read version acquisition shortens that conflict window significantly. It also ensures that the transaction sees the most recent committed state available at the time of its first read, rather than potentially stale data from when the transaction was created.

When the first read occurs, Transaction Builder gets the next read version directly from the [Sequencer](../../../glossary.md#sequencer) and coordinates with the [Gateway](../../../glossary.md#gateway) to obtain a lease for that version. This lease serves a crucial system-wide coordination function: it holds the window of readable versions open by preventing the system from garbage collecting data that active transactions might need.

The lease mechanism works by tracking all outstanding read version leases across the system. The oldest read version that's currently leased becomes the system's [minimum read version](../../../glossary.md#minimum-read-version)—storage servers cannot garbage collect any data at or after this version because some transaction might still need it. This creates a coordinated retention policy where data is kept as long as any transaction might read it.

Transaction Builder actively monitors lease expiration and renews leases when necessary, ensuring that its read version remains valid throughout the transaction's lifetime. If a lease cannot be renewed due to system policy limits, the transaction expires rather than risk reading inconsistent data. This lease management happens automatically in the background, balancing transaction needs with system resource management.

## Nested Transactions and State Stacking

Transaction Builder supports nested transactions, where a transaction can begin sub-transactions that have isolated change tracking while seeing the parent transaction's state. This capability is crucial for building complex application logic that needs transactional semantics at multiple levels.

When a nested transaction begins, it sees all the reads and writes from its parent transaction at that point in time. However, it maintains its own isolated read and write sets for new operations. The parent's state is pushed onto a stack, and the nested transaction starts with fresh, empty read/write maps for tracking its own changes.

When a nested transaction "commits," it's not a real distributed commit—it's a local merge operation where the nested transaction's writes are merged into the parent transaction. The reads are also merged because they represent data that was actually used in producing the committed writes.

If a nested transaction is rolled back, both its reads and writes are discarded entirely. This is crucial because those reads "didn't really happen" from the perspective of the overall transaction—none of that data was used to produce any writes that survived into the final transaction state. Therefore, those discarded reads won't be included in the conflict detection when the top-level transaction eventually commits.

Only the final, top-level transaction (after all nested commits and rollbacks are resolved) is sent to the [commit proxy](../../../glossary.md#commit-proxy) as a single, flattened transaction for distributed processing.

This approach provides significant performance benefits. Nested transactions require no network traffic, no coordination with other cluster components, and consume no distributed system resources. All nested transaction operations are purely local to the Transaction Builder process, dramatically reducing overhead compared to systems that treat each nested transaction as a separate distributed operation.

## Commit Coordination

When a transaction is ready to commit, Transaction Builder packages up the accumulated writes and read keys (not their values) and coordinates with a Commit Proxy. The read keys are needed for conflict detection—the system needs to know which keys the transaction read to determine if other transactions wrote to those keys in the meantime. The Transaction Builder selects an appropriate Commit Proxy and sends this transaction information, then waits for the commit result.

This coordination includes handling various commit outcomes: successful commits return a commit version, while conflicts or other errors provide specific error information that can be used for retry logic. The Transaction Builder manages this entire process, providing a simple interface to application code while handling the complex distributed coordination underneath.

## Integration with the Transaction System

Transaction Builder's embedded architecture enables sophisticated per-transaction optimization while participating in distributed coordination protocols. Its process-per-transaction model ensures perfect isolation between transactions while enabling complex nested transaction semantics and local performance optimizations.

## Component-Specific Responsibilities

Transaction Builder serves as the **per-transaction coordinator** with these specific responsibilities:

- **Process-Per-Transaction**: Dedicated process lifecycle management for individual transactions
- **Read-Your-Writes Cache**: Local write cache providing immediate consistency within transactions  
- **Storage Server Selection**: Intelligent routing and "horse racing" across storage replicas for optimal read performance
- **Version Management**: Lazy read version acquisition and lease coordination through Gateway
- **Nested Transaction Support**: Local state stacking for nested transaction semantics without distributed overhead
- **Commit Coordination**: Transaction preparation and handoff to Commit Proxy for distributed processing

> **Complete Flow**: For the full transaction processing sequence showing Transaction Builder's role in context, see **[Transaction Processing Deep Dive](../../../deep-dives/transactions.md)**.

## Related Components

- **[Gateway](gateway.md)**: Creates and manages Transaction Builder lifecycle
- **[Commit Proxy](../data-plane/commit-proxy.md)**: Receives transaction data for batch processing and durability
- **[Storage](../data-plane/storage.md)**: Serves versioned reads to Transaction Builder processes
- **[Sequencer](../data-plane/sequencer.md)**: Provides read versions for transaction consistency
