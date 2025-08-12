# Bedrock Architecture Deep Dive

Bedrock is an **embedded distributed database** that takes a different approach to data storage: running database components directly within your application processes while maintaining distributed ACID transactions and strong consistency. Instead of managing separate database servers, Bedrock components start with your application and provide distributed capabilities through function calls rather than network requests.

This approach builds on established concepts from existing systems. SQLite shows that embedded databases can be reliable and simple for single-machine use cases. DuckDB demonstrates how embedded analytical databases can perform well by eliminating network overhead. FoundationDB established distributed database architecture patterns through separation of concerns. Bedrock combines these approaches in a new configuration.

> **Navigation**: Start with [Transaction Overview](../quick-reads/transactions.md) for core concepts, or [Component Overview](../components/README.md) for individual component details.

## Table of Contents

- [Building on Established Foundations](#building-on-established-foundations)
  - [The Embedded + Distributed Approach](#the-embedded--distributed-approach)
- [Adapting FoundationDB's Architectural Patterns](#adapting-foundationdbs-architectural-patterns)
  - [Bedrock's Embedded Adaptation](#bedrocks-embedded-adaptation)
  - [What Embedded Deployment Enables](#what-embedded-deployment-enables)
- [The Components That Make It Work](#the-components-that-make-it-work)
  - [The Control Plane: Embedded Distributed Coordination](#the-control-plane-embedded-distributed-coordination)
  - [The Gateway Layer: Client Interface and Transaction Coordination](#the-gateway-layer-client-interface-and-transaction-coordination)
  - [The Service Management Layer: Infrastructure Coordination](#the-service-management-layer-infrastructure-coordination)
  - [The Data Plane: Exploring Embedded Performance Characteristics](#the-data-plane-exploring-embedded-performance-characteristics)
- [How It All Flows Together: The Embedded Approach in Practice](#how-it-all-flows-together-the-embedded-approach-in-practice)
  - [The Transaction Journey: Local First, Distributed When Needed](#the-transaction-journey-local-first-distributed-when-needed)
  - [The Embedded Approach: Local Read Optimization](#the-embedded-approach-local-read-optimization)
  - [Configuration Persistence: Using the System as Database](#configuration-persistence-using-the-system-as-database)
- [Resilience Through Embedded Coordination](#resilience-through-embedded-coordination)
  - [Coordinated Application-Database Recovery](#coordinated-application-database-recovery)
  - [Comprehensive Validation](#comprehensive-validation)
- [Bedrock's Current State and Future Direction](#bedrocks-current-state-and-future-direction)
  - [What Bedrock Provides Today](#what-bedrock-provides-today)
  - [Future Development](#future-development)
- [Navigation Guide](#navigation-guide)
  - [For New Developers](#for-new-developers)
  - [For Implementation Work](#for-implementation-work)
  - [For System Understanding](#for-system-understanding)
- [Conclusion: Exploring Database Architecture](#conclusion-exploring-database-architecture)
  - [What This Might Mean for Applications](#what-this-might-mean-for-applications)
  - [The Path Forward](#the-path-forward)

## Building on Established Foundations

To understand Bedrock's approach, it helps to consider the database systems that inspired it:

**SQLite** has processed many transactions across applications, showing that embedded databases can be reliable and simple. It eliminates configuration complexity and network overhead by running directly within applications. SQLite is designed for single-machine use cases.

**DuckDB** demonstrates how embedded analytical databases can perform well by eliminating network latency and optimizing for local data access patterns. Like SQLite, it performs well within single-machine constraints.

**FoundationDB** established approaches to distributed database architecture: separating control planes from data planes, implementing consensus algorithms, and designing for operational clarity. These patterns have been tested at scale in production systems.

**Bedrock combines these approaches**: taking SQLite and DuckDB's embedded simplicity and adapting FoundationDB's distributed architecture patterns. The goal is to create a database that runs within applications while providing distributed system capabilities.

### The Embedded + Distributed Approach

The traditional client-server database model creates a boundary between computation and storage. Every read crosses the network. Every write requires coordination with remote servers. Connection pools, query languages, serialization overhead, and network timeouts become fundamental parts of your application architecture.

Bedrock explores eliminating this boundary by making database components **part of your application**:

- **Local-first reads**: When data is available locally, reads can happen at memory speed
- **Direct function calls**: Function calls replace network protocols for many operations
- **Unified failure handling**: Database and application failures can be coordinated through the same supervision trees
- **Coordinated scaling**: Adding application nodes can automatically add database capacity

The key aspect of this approach is maintaining distributed capabilities alongside embedded benefits. Bedrock provides ACID transactions, strong consistency, and fault tolerance across multiple machines while running within application processes. This represents an exploration of whether embedded deployment can be compatible with distributed system guarantees.

## Adapting FoundationDB's Architectural Patterns

FoundationDB established principles for distributed database design, particularly **separation of concerns** in building systems that can be understood and maintained[^1]. Instead of monolithic architectures where components handle both data operations and cluster management, FoundationDB uses clear architectural division:

- **Control Plane**: Handles distributed coordination—leader election, recovery orchestration, cluster configuration management
- **Data Plane**: Focuses on core database operations—version sequencing, transaction processing, conflict resolution, data persistence
- **Gateway Layer**: Provides client interface and transaction coordination
- **Service Management**: Handles component lifecycle and infrastructure coordination

This separation enables FoundationDB to build a distributed database with clear operational characteristics. Each component has defined responsibilities, making the system more predictable and debuggable.

For details on Bedrock's implementation of this separation, see [Control Plane Overview](../quick-reads/control-plane-overview.md) and [Data Plane Overview](../quick-reads/data-plane-overview.md).

### Bedrock's Embedded Adaptation

Bedrock adapts FoundationDB's architectural patterns for embedded deployment. The same logical separation exists, but with a key difference: **all architectural layers can run within your application processes**.

This embedded deployment model creates new possibilities:

- **Local coordination**: Communication between control and data plane components can happen through local message passing rather than network protocols
- **Integrated failure handling**: FoundationDB's fail-fast recovery principles can work within the same supervision trees that manage application logic  
- **Application-aware optimization**: Components can potentially optimize based on local application access patterns and data placement preferences

The embedded model preserves FoundationDB's approach of simple abstractions backed by complex implementations, while testing whether embedded deployment can achieve different performance characteristics than traditional database architectures.

### What Embedded Deployment Enables

While FoundationDB provides the architectural foundation, Bedrock's embedded nature tests optimizations that may not be possible with traditional distributed databases:

- **Self-bootstrapping**: The system can potentially use its own transaction processing capabilities to persist configuration, reducing external dependencies
- **Local-first operations**: Reads from locally available data can happen at memory speed, with network operations only when necessary
- **Application lifecycle integration**: Database scaling, recovery, and operational tasks can coordinate with application deployment and management

## The Components That Make It Work

Understanding Bedrock's architecture means understanding how each component contributes to the embedded distributed database approach. These components run within your application processes while still forming a distributed system. This embedded deployment reduces network latency for many operations while testing optimizations that may not be practical when databases and applications are separated.

### The Control Plane: Embedded Distributed Coordination

The Control Plane handles distributed system challenges—cluster coordination, recovery orchestration, and maintaining system health. In traditional databases, this coordination layer adds network hops and operational complexity. Bedrock's embedded approach transforms coordination from a networking problem into a process management problem, reducing latency while maintaining distributed consensus guarantees.

**Coordinators** serve as the system's foundation, using Raft consensus to elect leadership and maintain cluster state. They handle persistence using Raft logs stored in DETS tables. While the system does write configuration to its own database during recovery, Coordinators currently continue to use Raft logs for their own persistence and configuration storage.

For more details on how cluster coordination works during startup, see [Cluster Startup](../deep-dives/cluster-startup.md).

When Coordinators run embedded within application processes, this creates opportunities not available to traditional databases. Applications can participate more directly in cluster decisions, coordinate recovery with application-specific knowledge, and influence data placement based on usage patterns—through local process communication rather than network protocols.

The **Director** orchestrates Bedrock's multi-phase recovery process from within the application cluster rather than as an external service. Instead of implementing custom logic for every failure scenario, the Director follows a systematic approach: discover current component state, determine what needs to happen to achieve consistency, coordinate the necessary changes, and validate success. When any step fails, the Director immediately exits—triggering fast local restart rather than relying on network-based health checking. This embedded fail-fast approach prevents the system from getting stuck in partially-recovered states while achieving faster recovery than external orchestration approaches.

The **Rate Keeper** provides distributed rate limiting and back-pressure management across the cluster. It monitors system performance and coordinates with other components to prevent overload conditions while maintaining optimal throughput. The Rate Keeper ensures that the system operates within sustainable performance boundaries during both normal operation and recovery scenarios.

For comprehensive coverage of the recovery process, see [Recovery Deep Dive](recovery.md).

### The Gateway Layer: Client Interface and Transaction Coordination

The Gateway Layer serves as the primary interface between applications and Bedrock's distributed components, providing an abstraction that makes distributed database operations work like local function calls while maintaining ACID guarantees and strong consistency.

**Transaction Builders** form the core of the Gateway's client interface. Instead of managing remote database connections through network protocols, applications interact with Transaction Builder processes that run directly within their supervision trees. This embedded approach transforms transactions from network abstractions into local data structures, enabling immediate read-your-writes consistency through local write caches and eliminating network latency for repeated data access within transactions.

The Gateway Layer coordinates between applications and the underlying Data Plane components through transaction lifecycle management. When applications begin transactions, the Gateway creates local Transaction Builder processes that accumulate writes in memory, coordinate with Sequencers for version assignment, and orchestrate the distributed commit process through Commit Proxies—all while presenting a simple functional interface to application code.

For detailed information about Gateway components, see [Gateway](../components/infrastructure/gateway.md).

### The Service Management Layer: Infrastructure Coordination

The Service Management Layer provides the infrastructure for deploying, monitoring, and coordinating distributed components across the cluster. This layer bridges the gap between application deployment and database component management, enabling Bedrock's embedded architecture to scale with application infrastructure.

**The Foreman** serves as the cluster-wide service orchestrator, responsible for deploying and managing database components according to the cluster configuration. It coordinates with application supervisors to deploy Database Plane components within application processes, manages service discovery and health monitoring, and ensures that database capacity scales appropriately with application deployment.

**Service Workers** handle the operational lifecycle of individual database components. Each database service—whether Storage Servers, Log Servers, or Commit Proxies—operates as a Service Worker that can be dynamically created, monitored, and restarted as needed. This worker architecture enables fine-grained operational control while maintaining the embedded deployment model.

The Service Management Layer enables Bedrock's operational characteristics: database components participate in application supervision trees while maintaining their distributed coordination requirements. This creates unified lifecycle management where database scaling, failure handling, and operational tasks are coordinated with application deployment and management processes.

For detailed information about service management, see [Foreman](../components/infrastructure/foreman.md).

### The Data Plane: Exploring Embedded Performance Characteristics

The Data Plane is where Bedrock's embedded approach aims to achieve performance benefits. While traditional databases must serve every read and write over network connections, Bedrock's Data Plane components run directly within application processes, changing the performance characteristics of database operations. This tests whether running database components locally can improve application responsiveness and throughput.

The **Sequencer** serves as the system's temporal authority, implementing Lamport clock semantics to ensure every transaction receives globally unique, causally-ordered version numbers. When a transaction begins, it gets a read version that guarantees a consistent snapshot of the database at that point in time. When it commits, it receives a commit version that establishes its place in the global ordering of all transactions. This temporal foundation enables Bedrock's [MVCC](../glossary.md#multi-version-concurrency-control) (Multi-Version Concurrency Control) capabilities, allowing multiple transactions to execute concurrently while maintaining the illusion that they ran in strict sequential order.

For detailed information about how versioning works, see [Sequencer](../components/data-plane/sequencer.md).

**Commit Proxies** orchestrate the distributed transaction commit process, batching multiple transactions together to improve throughput while maintaining correctness guarantees. Each commit proxy coordinates with Resolvers to detect conflicts, ensures that all Log servers acknowledge the transaction batch, and only then notifies clients that their transactions have committed. When these commit proxies run embedded within application nodes, the coordination overhead is reduced while still providing the guarantee that committed transactions are durably persisted across the cluster.

**Resolvers** implement the conflict detection logic that enables optimistic concurrency control. Using interval tree data structures, they detect when transactions have conflicting read and write operations across version ranges. This design allows transactions to execute without acquiring locks—they track what they've read and written, with conflict detection happening at commit time. This approach aims to reduce the complexity and deadlock risks of traditional locking schemes while providing strong consistency guarantees.

The storage layer leverages Bedrock's embedded architecture through two specialized components, each optimized for its role in the system. **Log Servers** provide the durability foundation, persisting every committed transaction in strict order. They require universal acknowledgment—every replica must confirm the write before the transaction is considered committed. This ensures that transaction data can be replayed from the logs to reconstruct any component's state, making recovery both possible and predictable.

**Storage Servers** represent where the embedded model may provide significant advantage. When Storage Servers run within your application processes, they serve reads directly from local memory, without network latency. They maintain multiple versions of each key, allowing transactions to read consistent snapshots at any committed version. By continuously following the transaction logs and incorporating committed changes, they provide read access that approaches local memory performance for application code. For applications with read-heavy workloads, this can provide response time improvements over traditional network-based database access.

## How It All Flows Together: The Embedded Approach in Practice

Bedrock's architecture shows how components collaborate to provide **distributed ACID transactions with local characteristics**. Unlike traditional databases where every operation involves network coordination, Bedrock starts local and only uses distributed coordination when necessary for correctness and durability.

### The Transaction Journey: Local First, Distributed When Needed

Every transaction in Bedrock demonstrates the embedded approach. Operations start within your application process and only involve the network when required for distributed consistency guarantees.

**Transaction Initiation** happens entirely within your application. When you call `Repo.transaction(fn repo -> ... end)`, Bedrock creates a Transaction Builder process right in your application's supervision tree. This isn't a remote database connection—it's a local component that can optimize for the common case where most operations will be local.

The **read phase** demonstrates the embedded approach. The first read operation acquires a consistent read version from the Sequencer, establishing the snapshot the transaction will see throughout its execution. Subsequent reads within the transaction often avoid network operations entirely. If you've written to a key and then read it back, that read is served from the local Transaction Builder cache. If you're reading data that's available in local Storage Servers, those reads happen with local memory access performance rather than network round-trip delays.

The **write phase** demonstrates Bedrock's optimistic concurrency control in an embedded context. Instead of acquiring distributed locks or sending writes across the network immediately, the transaction accumulates its writes in local memory. This provides immediate read-your-writes semantics within the transaction while avoiding the complexity and latency of distributed coordination. Multiple transactions execute concurrently without blocking each other, improving throughput for applications with concurrent workflows.

The **commit phase** is where the embedded architecture handles its distributed system requirements. The transaction is handed off to a Commit Proxy, which batches it with other transactions for efficiency. Resolvers check for conflicts using interval tree algorithms, while Log Servers prepare to durably persist the transaction. Only when all components have confirmed their readiness does the commit proxy commit the entire batch, ensuring ACID guarantees while processing multiple transactions efficiently.

**Completion** brings the benefits back to the local application. Clients receive their commit notifications through local message passing rather than network protocols, and transaction resources are cleaned up within the same supervision tree that hosts your application logic. This linear flow provides three properties: strict serialization (all transactions appear to execute in version order), optimistic concurrency (conflicts are detected at commit time rather than during reads), and universal durability (all replica Log servers acknowledge before success is reported).

### The Embedded Approach: Local Read Optimization

Bedrock's MVCC implementation is designed to be correct while testing how embedded deployment changes the performance characteristics of multi-version data access. Traditional distributed databases must serve every read over the network, but Bedrock serves many reads from local memory without network latency.

The Sequencer maintains several critical version markers that enable this local optimization: the **read version** that provides consistent snapshots, the **commit version** that establishes global ordering, the **last committed version** that tracks system progress, and the **minimum read version** that determines when old versions can be garbage collected. But because Storage Servers can run embedded in your application, they can serve reads at any version they've already applied locally, without network coordination.

This creates a performance profile that differs from traditional database architectures. When your application needs to read data it recently wrote, that read is served from the local Transaction Builder cache with low latency. When it needs data that's been committed but not locally cached, local Storage Servers serve it with memory access performance rather than network latency. Only when data isn't available locally does the system fall back to network requests, and even then it batches multiple keys into efficient requests.

The conflict detection algorithm is straightforward. Read-write conflicts occur when a transaction reads a key at version R, but that key was subsequently written by another transaction with commit version C where R < C <= current_commit. Write-write conflicts happen when two transactions in the same batch attempt to write to the same key. This logic, implemented with interval trees, enables strong consistency guarantees while maintaining high concurrency.

### Configuration Persistence: Using the System as Database

Bedrock addresses a common distributed systems problem: where to store configuration for a system that needs configuration to start. During recovery, the system writes its configuration to its own database.

After the Director orchestrates data plane recovery, the system uses its own transaction processing to store configuration. This approach serves as both configuration storage and an end-to-end test that validates the transaction pipeline works correctly.

The **system transaction pattern** stores cluster state:

```elixir
system_transaction = {
  nil,  # No reads required for system writes
  %{    # System configuration writes
    "\xff/system/config" => serialized_cluster_config,
    "\xff/system/epoch" => current_recovery_epoch,
    "\xff/system/last_recovery" => recovery_timestamp
  }
}
```

Currently, Coordinators use Raft logs for persistence and do not read configuration from Bedrock's storage during startup. If the system transaction fails, the recovery process fails fast, providing clear feedback that something needs attention rather than continuing in an inconsistent state.

## Resilience Through Embedded Coordination

Bedrock's approach to failure handling reflects its embedded nature—when your database runs within your application, recovery coordinates more tightly with application lifecycle, and failure detection is more immediate and precise.

### Coordinated Application-Database Recovery

**Component Failures** in embedded deployments create different opportunities and challenges compared to traditional database architectures:

- **Local Component Failures**: When Storage Servers or Transaction Builders fail within an application process, the Erlang/Elixir supervision tree can restart them immediately, often faster than network-based health checks could detect the failure.
- **Cross-Node Failures**: When entire application nodes fail, the remaining nodes can quickly detect the absence and coordinate both application-level and database-level recovery.
- **Network Partitions**: Raft consensus in the Coordinators ensures that minority partitions halt operations rather than risking split-brain scenarios, but the embedded deployment means that applications and their local database components fail together, reducing complex partial failure scenarios.

**Recovery Patterns** use the integration between application and database:

- **Unified Supervision Trees**: Database components participate in the same supervision hierarchies as application logic, enabling coordinated restarts and shared recovery strategies.
- **Application-Aware Recovery**: The recovery process can take advantage of application-specific knowledge about data importance, access patterns, and acceptable recovery trade-offs.
- **Fast Local Recovery**: Components that run embedded can restart in milliseconds rather than the seconds required for network-based service discovery and reconnection.

### Comprehensive Validation

The testing and validation strategy reflects Bedrock's embedded nature:

**Embedded Testing Advantages**: Because database components run within application processes, integration testing can exercise the full application-database stack without complex test environment setup. Property-based tests can validate MVCC and conflict resolution correctness using the same interfaces that application code uses.

**System Transaction as Integration Test**: The Director's configuration persistence serves as end-to-end system validation—if the system can successfully commit its own configuration, the entire transaction pipeline is working correctly. This provides continuous validation that traditional databases don't have.

## Bedrock's Current State and Future Direction

### What Bedrock Provides Today

Bedrock implements embedded distributed database concepts that build on established distributed systems research while testing new approaches enabled by embedded deployment:

- **Transaction Processing**: Multi-phase commit pipeline with ACID guarantees, optimistic concurrency control, and strong consistency across distributed nodes.
- **Local Read Optimization**: When data is available locally, reads happen with memory access latency rather than network latency, changing application performance characteristics.
- **Unified Application-Database Lifecycle**: Database components participate in the same supervision trees, configuration management, and deployment processes as application code.
- **Self-Bootstrapping Architecture**: The system can serve as its own configuration database, reducing external dependencies while providing end-to-end system validation.

### Future Development

Bedrock's development continues to test what's possible when databases and applications are more tightly integrated:

- **Performance Optimization**: Continued work on optimizing the transaction pipeline for both throughput and latency, testing how embedded deployment enables performance characteristics not available in traditional architectures.
- **Data Placement Algorithms**: Research into algorithms for automatically placing frequently accessed data on nodes where it's most needed, reducing network traffic and improving response times.
- **Application-Aware Operations**: Testing deeper integration between application logic and database operations, investigating optimizations based on application-specific knowledge of data access patterns and consistency requirements.

---

## Navigation Guide

### For New Developers

1. **Start Here**: [User's Perspective](../quick-reads/users-perspective.md) - How to interact with Bedrock
2. **Core Concepts**: [Transaction Basics](../quick-reads/transactions.md) - Quick reference for MVCC and versioning
3. **System Layout**: [Transaction System Layout](../quick-reads/transaction-system-layout.md) - Component relationships

### For Implementation Work  

1. **Component References**: [Individual Components](../components/README.md) - Detailed API and implementation docs
2. **Transaction Flow**: [Transaction Deep Dive](transactions.md) - Complete processing lifecycle
3. **Recovery Process**: [Recovery Deep Dive](recovery.md) - Multi-phase recovery system

### For System Understanding

1. **This Document**: Comprehensive architectural patterns and design principles
2. **Specialized Topics**: [Recovery Subsystem](../quick-reads/recovery/) - Detailed recovery phase documentation
3. **Reference**: [Glossary](../glossary.md) - Definitions and cross-linked concepts

---

## Conclusion: Exploring Database Architecture

Bedrock is an **embedded distributed system**. It combines the operational simplicity that has made SQLite successful, the performance optimizations demonstrated by DuckDB, and the distributed architecture principles established by FoundationDB—in a single system that runs inside your applications.

This approach represents a different way of thinking about the relationship between applications and data:

- **Simplified deployment**: No separate database servers to manage, while providing ACID transactions and strong consistency across multiple machines
- **Local-first performance**: Memory-speed local reads when possible, with automatic scaling across your cluster when needed
- **Unified lifecycle management**: Database components participate in your application's supervision trees and lifecycle, while maintaining isolation and consistency guarantees

### What This Might Mean for Applications

The embedded distributed approach tests new architectural possibilities:

**Local performance with global consistency**: Applications access frequently used data at memory speed while maintaining ACID guarantees across a distributed cluster, reducing the traditional trade-off between consistency and performance.

**Simplified operations**: Scaling your application automatically scales your database. Deploying your application deploys your database. Monitoring your application monitors your database. This reduces the operational complexity of managing separate systems.

**Integrated management**: Database failures, recovery, and operational tasks coordinate through the same supervision trees and management systems as your application logic, reducing the complexity of managing separate database and application operational models.

### The Path Forward

Bedrock tests whether embedded and distributed approaches can be complementary. By keeping data close to computation while maintaining distributed system guarantees, this architectural approach investigates whether applications can achieve both improved performance and resilience compared to traditional architectures.

The embedded distributed database approach tests whether new categories of applications might become practical by reducing the overhead and complexity of traditional database architectures.

This architectural foundation represents one approach to making such possibilities accessible.

## Footnotes

[^1]: [FoundationDB Paper](https://www.foundationdb.org/files/fdb-paper.pdf) - The seminal paper on FoundationDB's architecture and design principles
