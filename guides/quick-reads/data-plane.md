# Data Plane Components

The Data Plane implements the core transaction processing logic that enables ACID transactions, MVCC concurrency control, and distributed data persistence. These components handle the mechanical execution of transactions while maintaining consistency guarantees across the distributed system.

## What is the Data Plane?

The Data Plane focuses on the "operational" aspects of transaction processing: How do we assign version numbers? How do we detect conflicts between transactions? How do we ensure transactions are durably committed? How do we serve read operations efficiently?

Data Plane components deal with:

- **Transaction Processing**: Coordinating transaction execution from start to commit
- **Version Management**: Assigning and tracking globally unique transaction versions  
- **Conflict Detection**: Implementing MVCC to prevent conflicting transactions
- **Data Persistence**: Ensuring committed transactions survive system failures
- **Read Operations**: Serving consistent data snapshots across multiple versions

This contrasts with the Control Plane, which handles cluster coordination and recovery orchestration, and the Infrastructure layer, which manages client interfaces and worker processes.

## Transaction Processing Components

### [Sequencer](../deep-dives/architecture/data-plane/sequencer.md)

The singular version authority that assigns globally unique, monotonically increasing version numbers for MVCC. The Sequencer ensures every transaction receives a unique position in the global transaction ordering, enabling consistent snapshot isolation.

**Core Responsibilities:**

- Lamport clock implementation for global ordering
- Read version assignment for transaction snapshots
- Commit version assignment for durability ordering
- Version counter management across the distributed system

### [Commit Proxy](../deep-dives/architecture/data-plane/commit-proxy.md)

The transaction orchestrator that batches transactions from multiple clients and coordinates the distributed commit process. Commit Proxies implement the critical two-phase commit protocol that ensures atomicity across log servers while providing horizontal scaling for transaction processing.

**Core Responsibilities:**

- Transaction batching for efficiency optimization
- Read version request forwarding and caching
- Client connection management and load balancing
- Conflict resolution coordination with Resolvers
- Distributed commit protocol orchestration
- Durability guarantee enforcement across log replicas

### [Resolver](../deep-dives/architecture/data-plane/resolver.md)

The conflict detection engine that implements MVCC conflict detection for specific key ranges. Resolvers maintain version history and detect when concurrent transactions would violate isolation guarantees.

**Core Responsibilities:**

- Multi-version concurrency control implementation
- Interval tree maintenance for conflict detection
- Version history tracking across key ranges
- Intra-batch and inter-transaction conflict analysis

## Data Access Components

### [Log](../deep-dives/architecture/data-plane/log.md)

The persistent transaction log interface that maintains the complete history of committed transactions. Log servers provide the durability foundation that enables recovery and ensures transactions survive system failures.

**Core Responsibilities:**

- Transaction persistence and durability guarantees
- Sequential transaction ordering maintenance
- Recovery replay capability provision
- Tag-based transaction filtering for efficient storage coordination

### [Storage](../deep-dives/architecture/data-plane/storage.md)

The multi-version key-value storage interface that serves read operations across different transaction versions. Storage servers enable consistent snapshot reads while maintaining local state derived from transaction logs.

**Core Responsibilities:**

- MVCC read operation serving across multiple versions
- Transaction log following and state maintenance
- Key-value data organization and retrieval
- Version-specific snapshot consistency provision

The Data Plane receives coordination from the Control Plane during recovery and normal operations, while serving requests initiated through Infrastructure components.

## Transaction Flow Through Data Plane

1. **Transaction Initiation**: Commit Proxy retrieves read version from Sequencer for snapshot isolation
2. **Transaction Building**: Infrastructure layer accumulates reads and writes using Storage and Commit Proxy services
3. **Transaction Submission**: Transaction Builder submits complete transaction to Commit Proxy
4. **Conflict Detection**: Commit Proxy coordinates with Resolvers to detect version conflicts
5. **Commit Coordination**: Commit Proxy orchestrates two-phase commit across required Log servers
6. **Durability Confirmation**: Log servers acknowledge transaction persistence before commit confirmation
7. **Storage Propagation**: Storage servers follow transaction logs to maintain local MVCC state

## Consistency Guarantees

Data Plane components collectively provide several consistency guarantees:

**Snapshot Isolation**: Sequencer version assignment ensures transactions read consistent snapshots without seeing partial commits from concurrent transactions.

**Conflict Serializable isolation**: Resolver conflict detection prevents concurrent transactions from creating inconsistent states while maintaining optimistic concurrency benefits.

**Durability Assurance**: Commit Proxy coordination with Log servers ensures committed transactions persist across system failures and can be recovered.

**Version Ordering**: Global version assignment creates a total ordering that enables deterministic transaction replay and consistent recovery.

## Performance Characteristics

Data Plane components balance consistency with performance:

- **Version Assignment Bottleneck**: Single Sequencer trades availability for consistency, requiring careful placement and monitoring
- **Batching Optimization**: Commit Proxy batching amortizes coordination costs while adding latency for transaction grouping
- **Conflict Detection Scaling**: Multiple Resolvers handle different key ranges to distribute conflict detection workload
- **Read Scaling**: Multiple Commit Proxy and Storage instances provide horizontal read capacity
- **Write Durability Cost**: Universal Log acknowledgment ensures durability but limits write throughput to slowest replica

## Design Principles

The Data Plane follows these design principles:

- **Single Version Authority**: One Sequencer prevents version conflicts while requiring careful fault tolerance
- **Optimistic Concurrency**: Conflict detection at commit time reduces lock contention and deadlock risks  
- **Universal Acknowledgment**: All required replicas must acknowledge before commit completion
- **Separation of Concerns**: Clean interfaces between version management, conflict detection, and persistence

## See Also

- [Control Plane Overview](control-plane.md) - Cluster coordination components that orchestrate Data Plane recovery
- [Infrastructure Components](../deep-dives/architecture/infrastructure/README.md) - Client interface and worker management components that initiate Data Plane operations
- [Implementation Components](../deep-dives/architecture/implementations/README.md) - Concrete storage engines that implement Data Plane interfaces
- [Transactions Deep Dive](../deep-dives/transactions.md) - Detailed examination of transaction processing across Data Plane components
- [Recovery Deep Dive](../deep-dives/recovery.md) - Data Plane component recovery and coordination processes
