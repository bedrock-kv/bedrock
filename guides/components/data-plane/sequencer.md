# Sequencer: The Version Authority

The Sequencer serves as the authoritative source of version numbers in Bedrock, implementing a logical clock that enables MVCC conflict detection and maintains strict transaction ordering. It assigns globally unique, monotonically increasing version numbers that form the backbone of Bedrock's optimistic concurrency control system.

**Location**: [`lib/bedrock/data_plane/sequencer.ex`](../../../lib/bedrock/data_plane/sequencer.ex)

## The Need for Global Ordering

Distributed [MVCC](../../glossary.md#multi-version-concurrency-control) systems require a global timeline to determine transaction ordering and detect conflicts. Without a central authority for version assignment, different nodes might assign conflicting version numbers, leading to inconsistent conflict detection and potential data corruption. The Sequencer solves this by serving as the single source of truth for version assignment across the entire cluster.

Version numbers serve two distinct purposes in Bedrock. [Read versions](../../glossary.md#read-version) establish consistent snapshots that transactions use to read data at a specific point in time. [Commit versions](../../glossary.md#commit-version) establish the final ordering of transactions and enable conflict detection by providing reference points for when changes become visible.

## Version Assignment and Lamport Clock Semantics

The Sequencer tracks three distinct version counters to implement proper [Lamport clock](../../glossary.md#lamport-clock) semantics. The [known committed version](../../glossary.md#known-committed-version) represents the highest version confirmed as durably committed across all log servers, serving as the readable horizon for new transactions. [Read version](../../glossary.md#read-version) requests return this value, ensuring transactions see a consistent snapshot of committed data.

[Commit version](../../glossary.md#commit-version) assignment implements the Lamport clock mechanism by returning a version pair: the [last commit version](../../glossary.md#last-commit-version) and the newly assigned commit version. This pair forms the Lamport clock chain {last_commit_version, next_commit_version} that enables conflict detection—Resolvers can determine causality relationships between transactions based on these version boundaries. Each assignment updates the last commit version to the value just handed to the commit proxy.

Commit success reporting advances the known committed version when commit proxies confirm successful persistence to logs. This feedback loop ensures the readable horizon advances only when transactions are durably committed, maintaining strict consistency between version assignment and data visibility.

## State Management and Recovery

The Sequencer maintains three version counters for proper Lamport clock implementation: the known committed version (readable horizon), the last commit version (tracking what was handed to commit proxies), and the next commit version (counter for future assignments). The Director provides the initial known committed version during startup, establishing the correct starting point from the previous epoch.

Version gaps can occur when commit versions are assigned but transactions fail before completion. The Sequencer doesn't track these gaps explicitly—they're resolved naturally during recovery when the Director reconstructs the actual committed version history from persistent logs and provides the correct starting point for the new epoch.

## Performance Characteristics

The Sequencer initializes with all necessary state provided by the Director during startup, eliminating the need for separate recovery coordination. This ensures version assignment begins immediately from the correct position without additional setup phases.

The Sequencer's performance profile reflects its role as a lightweight coordination service. Version assignment operations are memory-only and CPU-bound, making them extremely fast. The Sequencer represents a serialization point in the system, with all version assignments flowing through this single component.

## Integration with Transaction Processing

The Sequencer integrates closely with the Commit Proxy, which coordinates transaction commits and requests both read and commit versions. Transaction Builders obtain read versions through the Gateway to establish consistent snapshots for their operations. Resolvers rely on the version chain information provided by commit version assignment to detect conflicts accurately.

This integration creates a version lifecycle that spans the entire transaction processing pipeline. Read versions flow from the Sequencer through the Gateway to Transaction Builders, establishing the snapshot point for transaction reads. Commit versions flow from the Sequencer through Commit Proxies to Resolvers, where they enable conflict detection. Success notifications flow back from Commit Proxies to the Sequencer, completing the cycle and maintaining version state consistency.

## Related Components

- **[Commit Proxy](commit-proxy.md)**
- **[Transaction Builder](transaction-builder.md)**
- **[Resolver](resolver.md)**
- **Director**: Control plane component that manages recovery
