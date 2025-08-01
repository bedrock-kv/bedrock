# Commit Proxy: The Transaction Orchestrator

The [Commit Proxy](../glossary.md#commit-proxy) is the central coordinator of Bedrock's [transaction](../glossary.md#transaction) commit process, responsible for [batching](../glossary.md#batching) transactions from multiple clients, orchestrating [conflict](../glossary.md#conflict) resolution, and ensuring [durable persistence](../glossary.md#durability-guarantee) across all [log](../glossary.md#log) servers. It transforms individual transaction requests into efficiently processed batches while maintaining strict consistency guarantees.

**Location**: [`lib/bedrock/data_plane/commit_proxy.ex`](../../lib/bedrock/data_plane/commit_proxy.ex)

## The Batching Strategy

Individual transactions arriving from [Transaction Builders](../glossary.md#transaction-builder) could be processed one at a time, but this would be highly inefficient. Each transaction would require its own conflict resolution phase, its own interaction with log servers, and its own coordination overhead. Instead, Commit Proxy batches multiple transactions together to amortize these costs.

Batching creates a fundamental trade-off between latency and throughput. Larger batches improve efficiency by spreading the fixed costs of conflict resolution and logging across more transactions, but they increase latency as transactions wait for batches to fill. Commit Proxy manages this trade-off through configurable policies that can commit batches based on size limits, time limits, or both.

The batching strategy also enables more sophisticated conflict resolution. When multiple transactions are processed together, the conflict resolution system can detect not only conflicts with previously committed transactions, but also conflicts within the current batch itself. This intra-batch conflict detection prevents incompatible transactions from being committed together.

Transactions within a batch maintain the same order in which they arrived at the Commit Proxy. This ordering is crucial for maintaining consistency—if two transactions in the same batch don't conflict with each other, their relative order must be preserved in the final committed state to ensure deterministic results.

## The Finalization Pipeline

When a batch is ready for processing, Commit Proxy executes a carefully orchestrated [finalization pipeline](../glossary.md#finalization-pipeline) that ensures atomicity across the distributed system. Each step builds on the previous ones, creating a chain of operations that either succeeds completely or fails cleanly.

The pipeline begins by creating a finalization plan that captures all the information needed for the batch, including the transactions, [version](../glossary.md#version) assignments, and [storage](../glossary.md#storage) team mappings. This plan serves as the coordination document that guides the remaining steps.

Next, transactions are prepared for conflict resolution by transforming them into the format that [Resolvers](../glossary.md#resolver) expect. This involves extracting read keys from Transaction Builders and write keys from the transaction payloads, creating summaries that focus on conflict detection rather than data values.

The conflict resolution phase distributes transaction summaries to the appropriate Resolvers based on key ranges. Each Resolver checks for conflicts within its range and returns either approval or rejection for each transaction. Transactions that conflict with previously committed transactions or with other transactions in the same batch are marked for abortion.

Aborted transactions receive immediate error responses, allowing clients to retry quickly rather than waiting for the entire batch to complete. This fail-fast approach minimizes wasted work and improves overall system responsiveness.

The remaining successful transactions are then prepared for logging by organizing them according to storage team tags. This organization ensures that each log server receives only the transactions it needs to store, based on the tag coverage configuration.

The logging phase pushes transactions to all required log servers in parallel. This is the critical durability step—transactions are not considered committed until every required log server acknowledges receipt. If any log server fails to acknowledge, the entire batch fails and triggers [recovery](../glossary.md#recovery).

After successful logging, the Commit Proxy notifies the [Sequencer](../glossary.md#sequencer) about the successful commit, enabling the Sequencer to update its tracking of committed versions. Finally, successful transaction clients receive their commit version responses.

## Conflict Resolution Coordination

Commit Proxy acts as the intermediary between Transaction Builders and Resolvers, transforming transaction data into the format needed for conflict detection. Transaction Builders send complete transaction information including read keys, write keys and values, and read versions. Resolvers only need the keys and versions for conflict detection, so Commit Proxy filters out unnecessary data.

The coordination becomes complex when transactions span multiple key ranges, requiring interaction with multiple Resolvers. Commit Proxy manages this fan-out and fan-in process, ensuring that all relevant Resolvers have a chance to examine the transactions and that their responses are properly aggregated.

Intra-batch conflict detection adds another layer of complexity. Resolvers must not only check for conflicts with previously committed transactions, but also ensure that transactions within the current batch don't conflict with each other. This requires careful coordination of the conflict resolution process across all affected Resolvers.

## Durability Guarantees and Log Coordination

The durability guarantee is absolute: transactions are not committed unless all required log servers acknowledge receipt. This all-or-nothing approach ensures that committed transactions can always be recovered, even if some log servers fail later.

Commit Proxy determines which log servers need each transaction based on the tag coverage configuration. Different transactions may go to different sets of log servers, but within each set, acknowledgment must be universal. This approach enables storage servers to pull transactions from any available log server while maintaining consistency.

The parallel logging approach minimizes durability latency by sending transactions to all log servers simultaneously rather than sequentially. However, the requirement for universal acknowledgment means that durability is limited by the slowest log server in the required set.

## Recovery and Error Handling

Commit Proxy uses a fail-fast recovery model similar to other Bedrock components. When unrecoverable errors occur—such as Resolver unavailability or insufficient log acknowledgments—the Commit Proxy exits immediately, triggering [Director](../glossary.md#director)-coordinated recovery.

This approach simplifies error handling logic by avoiding complex partial recovery scenarios. Instead of trying to salvage partial state, the system replaces the failed Commit Proxy with a fresh instance that starts with clean state.

In-flight transactions are lost during recovery, requiring clients to retry their operations. This trade-off between availability and consistency is deliberate—Bedrock chooses to maintain strict consistency even at the cost of requiring client retries.

## Performance Tuning and Trade-offs

The key performance tuning parameters control the batching behavior. Maximum batch size affects throughput versus resource usage—larger batches process more efficiently but consume more memory and increase conflict probability. Maximum batch latency affects responsiveness versus efficiency—shorter timeouts reduce transaction latency but may prevent batches from reaching optimal size.

These parameters must be tuned based on workload characteristics. High-throughput workloads with many small transactions benefit from larger batches and longer timeouts. Low-latency workloads with fewer transactions benefit from smaller batches and shorter timeouts.

The finalization pipeline depth creates inherent latency that cannot be eliminated through tuning. Each step in the pipeline adds processing time, creating a baseline latency that affects all transactions regardless of batching parameters.

## Integration with the Transaction System

Commit Proxy sits at the center of the transaction commit process, coordinating with multiple other components. It receives transactions from Transaction Builders, coordinates with Resolvers for conflict detection, pushes to Log servers for durability, and reports to the Sequencer for version tracking.

These integration points are designed to be resilient and performant. Commit Proxy handles component failures gracefully through its fail-fast model, implements appropriate timeouts to prevent resource leaks, and provides telemetry that helps operators understand transaction processing performance.

The component also coordinates with the Director during recovery, accepting new transaction system layouts and transitioning from locked to running mode when recovery completes.

For a detailed walkthrough of the complete transaction flow including the Commit Proxy's role, see **[Transaction Processing Deep Dive](../knowlege_base/02-deep/transactions-deep.md)**.

## Related Components

- **[Transaction Builder](transaction-builder.md)**
- **[Sequencer](sequencer.md)**
- **[Resolver](resolver.md)**
- **[Log](log.md)**
- **[Director](../control-plane/director.md)**