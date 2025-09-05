# Sequencer

The Sequencer serves as the authoritative source of version numbers in Bedrock, implementing a logical clock that enables MVCC conflict detection and maintains strict transaction ordering. It assigns globally unique, monotonically increasing version numbers that form the backbone of Bedrock's optimistic concurrency control system.

**Location**: [`lib/bedrock/data_plane/sequencer.ex`](../../../lib/bedrock/data_plane/sequencer.ex)

## The Need for Global Ordering

Distributed [MVCC](../../../glossary.md#multi-version-concurrency-control) systems require a global timeline to determine transaction ordering and detect conflicts. Without a central authority for version assignment, different nodes might assign conflicting version numbers, leading to inconsistent conflict detection and potential data corruption. The Sequencer solves this by serving as the single source of truth for version assignment across the entire cluster.

Version numbers serve two distinct purposes in Bedrock. [Read versions](../../../glossary.md#read-version) establish consistent snapshots that transactions use to read data at a specific point in time. [Commit versions](../../../glossary.md#commit-version) establish the final ordering of transactions and enable conflict detection by providing reference points for when changes become visible.

## Version Assignment and Lamport Clock Semantics

The Sequencer tracks three distinct version counters to implement proper [Lamport clock](../../../glossary.md#lamport-clock) semantics. The [known committed version](../../../glossary.md#known-committed-version) represents the highest version confirmed as durably committed across all log servers, serving as the readable horizon for new transactions. [Read version](../../../glossary.md#read-version) requests return this value, ensuring transactions see a consistent snapshot of committed data.

[Commit version](../../../glossary.md#commit-version) assignment implements the Lamport clock mechanism by returning a version pair: the [last commit version](../../../glossary.md#last-commit-version) and the newly assigned commit version. This pair forms the Lamport clock chain {last_commit_version, next_commit_version} that enables conflict detection—Resolvers can determine causality relationships between transactions based on these version boundaries. Each assignment updates the last commit version to the value just handed to the commit proxy.

Commit success reporting advances the known committed version when commit proxies confirm successful persistence to logs. This feedback loop ensures the readable horizon advances only when transactions are durably committed, maintaining strict consistency between version assignment and data visibility.

## State Management and Recovery

The Sequencer maintains three version counters for proper Lamport clock implementation: the known committed version (readable horizon), the last commit version (tracking what was handed to commit proxies), and the next commit version (counter for future assignments). The Director provides the initial known committed version during startup, establishing the correct starting point from the previous epoch.

Version gaps can occur when commit versions are assigned but transactions fail before completion. The Sequencer doesn't track these gaps explicitly—they're resolved naturally during recovery when the Director reconstructs the actual committed version history from persistent logs and provides the correct starting point for the new epoch.

## Performance Characteristics

The Sequencer initializes with all necessary state provided by the Director during startup, eliminating the need for separate recovery coordination. This ensures version assignment begins immediately from the correct position without additional setup phases.

The Sequencer's performance profile reflects its role as a lightweight coordination service. Version assignment operations are memory-only and CPU-bound, making them extremely fast. The Sequencer represents a serialization point in the system, with all version assignments flowing through this single component.

## Integration with Transaction Processing

The Sequencer's version assignment creates the foundation for all transaction ordering and conflict detection across the distributed system. Its lightweight, memory-only operations ensure that version assignment doesn't become a bottleneck while maintaining the strict consistency required for MVCC correctness.

## Component-Specific Responsibilities

Sequencer serves as the **global version authority** with these specific responsibilities:

- **Read Version Assignment**: Provides consistent snapshot points using the known committed version
- **Commit Version Assignment**: Issues globally unique, monotonically increasing commit versions with Lamport clock chains
- **Version State Tracking**: Maintains known committed, last commit, and next commit version counters
- **Consistency Coordination**: Ensures read versions only reflect durably committed transactions through feedback loops
- **Recovery Integration**: Accepts initial version state from Director to maintain correct version progression

> **Complete Flow**: For the full transaction processing sequence showing Sequencer's role in context, see **[Transaction Processing Deep Dive](../../../deep-dives/transactions.md)**.

## Related Components

- **[Gateway](../infrastructure/gateway.md)**: Requests read versions from Sequencer for Transaction Builder processes
- **[Transaction Builder](../infrastructure/transaction-builder.md)**: Obtains read versions through Gateway for transaction consistency
- **[Commit Proxy](commit-proxy.md)**: Requests commit versions and reports successful commits to Sequencer
- **[Resolver](resolver.md)**: Uses Sequencer version chains for conflict detection analysis
- **[Director](../control-plane/director.md)**: Control plane component that provides initial version state during recovery
