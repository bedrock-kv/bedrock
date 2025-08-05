# Resolver: The Conflict Detection Engine

The [Resolver](../../glossary.md#resolver) implements Bedrock's [Multi-Version Concurrency Control (MVCC)](../../glossary.md#multi-version-concurrency-control) [conflict](../../glossary.md#conflict) detection, serving as the arbiter that determines which [transactions](../../glossary.md#transaction) can safely [commit](../../glossary.md#commit) together. Each Resolver covers a specific [key range](../../glossary.md#key-range) and maintains [version](../../glossary.md#version) history to detect when transactions would violate serialization if allowed to proceed.

**Location**: [`lib/bedrock/data_plane/resolver.ex`](../../../lib/bedrock/data_plane/resolver.ex)

## Optimistic Concurrency and Conflict Detection

Bedrock uses [Optimistic Concurrency Control (OCC)](../../glossary.md#optimistic-concurrency-control), where transactions proceed without acquiring locks and conflicts are detected only at commit time. This approach maximizes concurrency and eliminates deadlocks, but it requires sophisticated conflict detection to maintain correctness. The fundamental challenge is determining whether a set of transactions, if committed together, would produce a result equivalent to some serial execution of those same transactions.

Resolvers detect two primary types of conflicts that can violate serializability. Read-write conflicts occur when a transaction reads a key, then another transaction commits a write to that same key before the first transaction commits—the reading transaction based its decisions on stale data. Write-write conflicts happen when two transactions attempt to write to overlapping key ranges, which would produce arbitrary results depending on [storage](../../glossary.md#storage) server timing.

## Version History Through Interval Trees

Resolvers maintain an interval tree that tracks which key ranges were written at which versions. This structure enables efficient conflict detection by answering whether any writes occurred to overlapping key ranges after a transaction's [read version](../../glossary.md#read-version). The tree must be carefully pruned to prevent unbounded memory growth by removing entries for versions older than the [minimum read version](../../glossary.md#minimum-read-version) across active transactions.

The [Commit Proxy](../../glossary.md#commit-proxy) handles all key range partitioning and routing, sending each Resolver only the transactions relevant to its assigned key space. Resolvers don't need to know their specific key ranges—they simply process whatever transactions they receive, trusting that the Commit Proxy has already performed the necessary filtering based on the [transaction system layout](../../glossary.md#transaction-system-layout) established during [recovery](../../glossary.md#recovery).

## Resolution Process and Abort Strategy

When a Resolver receives a [batch](../../glossary.md#batch) of transactions from the Commit Proxy, it processes each transaction in order to detect conflicts. For read-write conflicts, it checks whether any read keys overlap with key ranges that were written after the transaction's read version. For write-write conflicts, it identifies cases where multiple transactions attempt to modify overlapping key ranges.

When conflicts are detected, transactions are processed in order and conflicting transactions are simply added to an aborted list by their index position. This preserves transaction arrival order and provides predictable behavior.

The resolution process produces a list of transaction indices that must be aborted due to conflicts. The Commit Proxy uses this information to notify affected clients while allowing non-conflicting transactions to proceed to the logging phase.

## Recovery, Version Ordering, and Performance

During recovery, Resolvers start in locked mode and must rebuild their interval tree from committed transaction logs. The recovery process pulls transactions from logs and applies their writes to reconstruct which key ranges were written at which versions. Resolvers remain locked until explicitly unlocked through the recovery process, ensuring proper coordination during cluster recovery scenarios.

In normal operation, Resolvers expect transactions to arrive in version order. When transactions arrive out of order, the Resolver maintains a waiting queue indexed by version number, holding later transactions until earlier versions are processed. This ensures consistent conflict detection regardless of network timing variations.

The AVL tree-based interval structure provides logarithmic lookup times for conflict checks, making the process scalable with large amounts of historical data. Memory usage is managed through pruning based on the minimum read version across active transactions, ensuring Resolvers don't accumulate unbounded state while maintaining the history needed for correct conflict detection.

## Integration with the Transaction System

Resolvers integrate closely with the Commit Proxy, which coordinates the overall transaction commit process. The Commit Proxy sends batches of transactions to the appropriate Resolvers based on key ranges, then aggregates their responses to determine which transactions can proceed.

The component also coordinates with the Sequencer to understand version ordering and with Storage servers to ensure that conflict resolution decisions are reflected in the final committed state. This integration ensures that the optimistic concurrency control system maintains strict serializability despite its distributed nature.

## Related Components

- **[Commit Proxy](commit-proxy.md)**
- **[Sequencer](sequencer.md)**
- **[Storage](../data-plane/storage.md)**
- **Director**: Control plane component that manages recovery
