# Bedrock Architecture Deep Dive

Bedrock is an **embedded distributed database** that explores a different approach to data storage: running database components directly within your application processes while maintaining distributed ACID transactions and strong consistency. Rather than managing separate database servers, Bedrock components start with your application and provide distributed capabilities through function calls instead of network requests.

This approach builds on proven concepts from established systems. SQLite demonstrated the power of embedded databases for single-machine use cases. DuckDB showed how embedded analytical databases could achieve exceptional performance by eliminating network overhead. FoundationDB proved that distributed databases could be both sophisticated and operationally manageable through careful architectural separation of concerns. Bedrock attempts to combine these proven approaches in a new way.

> **Navigation**: Start with [Transaction Overview](../quick-reads/transactions.md) for core concepts, or [Component Overview](../components/) for individual component details.

## Building on Established Foundations

To understand Bedrock's approach, it helps to consider the database systems that inspired it:

**SQLite** has processed trillions of transactions across countless applications, proving that embedded databases can be both reliable and simple. Its approach eliminates configuration complexity and network overhead by running directly within applications. However, SQLite is designed for single-machine use cases.

**DuckDB** has shown how embedded analytical databases can achieve exceptional performance by eliminating network latency and optimizing for local data access patterns. Like SQLite, it provides impressive performance within single-machine constraints.

**FoundationDB** pioneered sophisticated approaches to distributed database architecture: separating control planes from data planes, implementing robust consensus algorithms, and designing for operational clarity. These battle-tested patterns have been proven at massive scale across many production systems.

**Bedrock attempts to combine these approaches**: taking SQLite's embedded simplicity, incorporating DuckDB's performance optimizations, and adapting FoundationDB's distributed architecture patterns. The goal is to create a database that runs within your applications while providing distributed system capabilities.

### The Embedded + Distributed Approach

The traditional client-server database model creates a boundary between computation and storage. Every read crosses the network. Every write requires coordination with remote servers. Connection pools, query languages, serialization overhead, and network timeouts become fundamental parts of your application architecture.

Bedrock explores eliminating this boundary by making database components **part of your application**:

- **Local-first reads**: When data is available locally, reads can happen at memory speed
- **Direct function calls**: Function calls replace network protocols for many operations
- **Unified failure handling**: Database and application failures can be coordinated through the same supervision trees
- **Coordinated scaling**: Adding application nodes can automatically add database capacity

The key aspect of this approach is maintaining distributed capabilities alongside embedded benefits. Bedrock provides ACID transactions, strong consistency, and fault tolerance across multiple machines while running within application processes. This represents an exploration of whether embedded deployment can be compatible with distributed system guarantees.

## Adapting FoundationDB's Architectural Patterns

FoundationDB established important principles for distributed database design, particularly the value of **separation of concerns** in building systems that can be understood and maintained¹. Rather than monolithic architectures where components handle both data operations and cluster management, FoundationDB demonstrated the benefits of clear architectural division:

- **Control Plane**: Handles distributed coordination—leader election, recovery orchestration, version management
- **Data Plane**: Focuses on core database operations—transaction processing, conflict resolution, data persistence

This separation enabled FoundationDB to build a distributed database that was sophisticated in its capabilities yet operationally manageable. Each component had clear, well-defined responsibilities, making the system more predictable and debuggable under various conditions.

### Bedrock's Embedded Adaptation

Bedrock adapts FoundationDB's architectural patterns for embedded deployment. The same logical separation exists, but with a key difference: **both planes can run within your application processes**.

This embedded deployment model creates new possibilities:

- **Local coordination**: Communication between control and data plane components can happen through local message passing rather than network protocols
- **Integrated failure handling**: FoundationDB's fail-fast recovery principles can work within the same supervision trees that manage application logic  
- **Application-aware optimization**: Components can potentially optimize based on local application access patterns and data placement preferences

The embedded model preserves FoundationDB's approach of simple abstractions backed by sophisticated implementations, while exploring whether embedded deployment can achieve different performance characteristics than traditional database architectures.

### What Embedded Deployment Enables

While FoundationDB provides the architectural foundation, Bedrock's embedded nature explores optimizations that may not be possible with traditional distributed databases:

- **Self-bootstrapping**: The system can potentially use its own transaction processing capabilities to persist configuration, reducing external dependencies
- **Local-first operations**: Reads from locally available data can happen at memory speed, with network operations only when necessary
- **Application lifecycle integration**: Database scaling, recovery, and operational tasks can coordinate with application deployment and management

## The Components That Make It Work

Understanding Bedrock's architecture means understanding how each component contributes to the embedded distributed database approach. These components can run within your application processes while still forming a distributed system. This embedded deployment aims to reduce network latency for many operations while exploring optimizations that may not be practical when databases and applications are separated.

### The Control Plane: Embedded Distributed Coordination

The Control Plane handles the challenges that make distributed systems complex—cluster coordination, recovery orchestration, and maintaining system health. In traditional databases, this coordination layer adds network hops and operational complexity. Bedrock's embedded approach aims to transform coordination from a networking problem into a process management problem, potentially reducing latency while maintaining distributed consensus guarantees.

**Coordinators** serve as the system's foundation, using Raft consensus to elect leadership and maintain cluster state. An interesting aspect of their design is how they handle persistence: rather than requiring external storage systems, they can leverage Bedrock's own key-value storage once it becomes available. During cold starts, Coordinators begin with minimal Raft logs, but as the system becomes operational, they can migrate to using Bedrock's own transaction capabilities for persistence. This self-bootstrapping approach reduces external dependencies while ensuring that the coordination layer benefits from the same durability and consistency guarantees as the main system.

For more details on how cluster coordination works during startup, see [Cluster Startup](../deep-dives/cluster-startup.md).

When Coordinators run embedded within application processes, this creates opportunities that may not be available to traditional databases. Applications can potentially participate more directly in cluster decisions, coordinate recovery with application-specific knowledge, and influence data placement based on usage patterns—through local process communication rather than network protocols.

The **Director** orchestrates Bedrock's multi-phase recovery process from within the application cluster rather than as an external service. Rather than implementing custom logic for every failure scenario, the Director follows a systematic approach: discover current component state, determine what needs to happen to achieve consistency, coordinate the necessary changes, and validate success. When any step fails, the Director immediately exits—triggering fast local restart rather than relying on network-based health checking. This embedded fail-fast approach aims to prevent the system from getting stuck in partially-recovered states while potentially achieving faster recovery than external orchestration approaches.

For comprehensive coverage of the recovery process, see [Recovery Deep Dive](recovery.md).

### The Data Plane: Exploring Embedded Performance Characteristics

The Data Plane is where Bedrock's embedded approach aims to achieve its primary performance benefits. While traditional databases must serve every read and write over network connections, Bedrock's Data Plane components can run directly within application processes, potentially changing the performance characteristics of database operations. This is where the embedded architecture hypothesis is tested—whether running database components locally can improve application responsiveness and throughput.

The **Sequencer** serves as the system's temporal authority, implementing Lamport clock semantics to ensure every transaction receives globally unique, causally-ordered version numbers. When a transaction begins, it gets a read version that guarantees a consistent snapshot of the database at that point in time. When it commits, it receives a commit version that establishes its place in the global ordering of all transactions. This temporal foundation enables Bedrock's [MVCC](../glossary.md#mvcc) (Multi-Version Concurrency Control) capabilities, allowing multiple transactions to execute concurrently while maintaining the illusion that they ran in strict sequential order.

For detailed information about how versioning works, see [Sequencer](../components/control-plane/sequencer.md).

**Transaction Builders** demonstrate the embedded database approach. In traditional databases, transactions are remote abstractions—entities managed by server processes that applications communicate with through network protocols. In Bedrock, Transaction Builders run directly within your application's supervision tree, making transactions work more like local data structures while maintaining ACID guarantees.

This embedded approach aims to create performance benefits. Transaction Builders maintain local write caches, providing read-your-writes consistency without network latency. When your application writes to a key and immediately reads it back—a common pattern in business logic—that read can be served from local memory in microseconds rather than milliseconds. For applications that repeatedly access the same data within a transaction, this may provide significant performance improvements over traditional database architectures.

**Commit Proxies** orchestrate the distributed transaction commit process in a way that attempts to leverage local deployment benefits. Rather than committing each transaction individually, they batch multiple transactions together, potentially improving throughput while maintaining correctness guarantees. Each proxy coordinates with Resolvers to detect conflicts, ensures that all Log servers acknowledge the transaction batch, and only then notifies clients that their transactions have committed. Because these proxies can run embedded in your application nodes, the coordination overhead may be reduced while still providing the guarantee that committed transactions are durably persisted.

**Resolvers** implement the conflict detection logic that enables optimistic concurrency control. Using interval tree data structures, they detect when transactions have conflicting read and write operations across version ranges. This design allows transactions to execute without acquiring locks—they track what they've read and written, with conflict detection happening at commit time. This approach aims to reduce the complexity and deadlock risks of traditional locking schemes while providing strong consistency guarantees.

The storage layer leverages Bedrock's embedded architecture through two specialized components, each optimized for its role in the system. **Log Servers** provide the durability foundation, persisting every committed transaction in strict order. They require universal acknowledgment—every replica must confirm the write before the transaction is considered committed. This ensures that transaction data can be replayed from the logs to reconstruct any component's state, making recovery both possible and predictable.

**Storage Servers** represent where the embedded model may provide its most significant advantage. When Storage Servers run within your application processes, they can potentially serve reads directly from local memory, without network latency. They maintain multiple versions of each key, allowing transactions to read consistent snapshots at any committed version. By continuously following the transaction logs and incorporating committed changes, they aim to provide read access that approaches local memory speeds for application code. For applications with read-heavy workloads, this could potentially improve response times from milliseconds to microseconds.

## How It All Flows Together: The Embedded Approach in Practice

Bedrock's architecture design shows how components collaborate to provide **distributed ACID transactions with local characteristics**. Unlike traditional databases where every operation involves network coordination, Bedrock attempts to start local and only use distributed coordination when necessary for correctness and durability.

### The Transaction Journey: Local First, Distributed When Needed

Every transaction in Bedrock demonstrates the embedded approach. Operations start within your application process and only involve the network when required for distributed consistency guarantees.

**Transaction Initiation** happens entirely within your application. When you call `Repo.transaction(fn repo -> ... end)`, Bedrock creates a Transaction Builder process right in your application's supervision tree. This isn't a remote database connection—it's a local component that can optimize for the common case where most operations will be local.

The **read phase** demonstrates the embedded approach. The first read operation acquires a consistent read version from the Sequencer, establishing the snapshot the transaction will see throughout its execution. Subsequent reads within the transaction can often avoid network operations entirely. If you've written to a key and then read it back, that read can be served from the local Transaction Builder cache. If you're reading data that's available in local Storage Servers, those reads can happen with microsecond latency rather than millisecond network round-trips.

The **write phase** demonstrates Bedrock's optimistic concurrency control in an embedded context. Rather than acquiring distributed locks or sending writes across the network immediately, the transaction accumulates its writes in local memory. This provides immediate read-your-writes semantics within the transaction while avoiding the complexity and latency of distributed coordination. Multiple transactions can execute concurrently without blocking each other, potentially improving throughput for applications with concurrent workflows.

The **commit phase** is where the embedded architecture handles its distributed system requirements. The transaction is handed off to a Commit Proxy, which batches it with other transactions for efficiency. Resolvers check for conflicts using interval tree algorithms, while Log Servers prepare to durably persist the transaction. Only when all components have confirmed their readiness does the proxy commit the entire batch, ensuring ACID guarantees while processing multiple transactions efficiently.

**Completion** brings the benefits back to the local application. Clients receive their commit notifications through local message passing rather than network protocols, and transaction resources are cleaned up within the same supervision tree that hosts your application logic. This simple linear flow provides three crucial properties: strict serialization (all transactions appear to execute in version order), optimistic concurrency (conflicts are detected at commit time rather than during reads), and universal durability (all replica Log servers acknowledge before success is reported).

### The Embedded Approach: Local Read Optimization

Bedrock's MVCC implementation aims to be correct while exploring how embedded deployment might change the performance characteristics of multi-version data access. Traditional distributed databases must serve every read over the network, but Bedrock attempts to serve many reads from local memory without network latency.

The Sequencer maintains several critical version markers that enable this local optimization: the **read version** that provides consistent snapshots, the **commit version** that establishes global ordering, the **last committed version** that tracks system progress, and the **minimum read version** that determines when old versions can be garbage collected. But because Storage Servers can run embedded in your application, they can serve reads at any version they've already applied locally, without network coordination.

This aims to create a performance profile that differs from traditional database architectures. When your application needs to read data it recently wrote, that read can be served from the local Transaction Builder cache in microseconds. When it needs data that's been committed but not locally cached, local Storage Servers can potentially serve it with memory access latency rather than network latency. Only when data isn't available locally does the system fall back to network requests, and even then it can batch multiple keys into efficient requests.

The conflict detection algorithm is straightforward. Read-write conflicts occur when a transaction reads a key at version R, but that key was subsequently written by another transaction with commit version C where R < C <= current_commit. Write-write conflicts happen when two transactions in the same batch attempt to write to the same key. This simple logic, implemented efficiently with interval trees, enables strong consistency guarantees while maintaining high concurrency.

### Self-Bootstrapping: The System as Its Own Database

An interesting aspect of Bedrock's design is how it addresses the classic distributed systems problem: where do you store the configuration for a system that needs configuration to start? The embedded approach enables the system to serve as its own database.

During **cold start**, Coordinators begin with minimal configuration and ephemeral Raft logs. As the Director successfully orchestrates data plane recovery, the system can start using its own transaction processing capabilities to persist its configuration. This approach serves as both practical configuration storage and an end-to-end test that validates the entire transaction pipeline works correctly.

The **system transaction pattern** is straightforward:
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

For **warm starts**, Coordinators read their configuration from local storage using the same interfaces that application code uses. This self-bootstrapping design reduces external dependencies while ensuring that the coordination layer benefits from the same durability and consistency guarantees as the main system. If the system transaction fails, the recovery process fails fast, providing clear feedback that something needs attention rather than continuing in an inconsistent state.

## Distribution Strategy: Local Performance with Distributed Guarantees

Bedrock's approach to data distribution follows a core principle: **keep data as close to applications as possible while maintaining distributed system guarantees**. Rather than forcing a choice between local performance and distributed consistency, Bedrock explores whether both can be achieved together.

### Locality-Aware Placement

Unlike traditional distributed databases that treat all nodes equally, Bedrock can potentially optimize data placement based on **where applications actually run**. Since database components are embedded within application processes, the system can make decisions about data locality:

- **Hot data locally**: Frequently accessed keys can be prioritized for storage on the same nodes where applications need them most
- **Cold data distributed**: Less frequently accessed data can be placed across the cluster for durability without impacting local performance
- **Application-aware replication**: The system can replicate data based on actual application usage patterns rather than generic distribution algorithms

This creates a **tiered performance model** where:
1. **Local reads** (same process): Microsecond memory access
2. **Node-local reads** (same machine, different process): Sub-millisecond local network
3. **Cluster reads** (different machines): Standard network latency, but only when necessary

### Embedded-Optimized Replication

The replication strategy balances the embedded model's local performance advantages with distributed system durability requirements:

**Transaction logs** are replicated across multiple Log servers using traditional distributed consensus, ensuring that committed data survives node failures. But because Log servers can run embedded within application nodes, even this replication overhead is minimized.

**Storage data** uses intelligent key range replication that takes advantage of embedded deployment. Storage servers can run co-located with the applications that need their data most frequently, while still maintaining sufficient replicas across the cluster for fault tolerance.

### Performance Through Embedding

Bedrock's performance optimizations are designed around the embedded model's unique characteristics, creating performance profiles that traditional distributed databases simply cannot achieve:

**Local-First Processing**: The embedded approach enables a performance hierarchy that eliminates network latency when possible:
- **Write caching**: Transaction Builders accumulate writes locally, providing instant read-your-writes consistency
- **Local reads**: Storage Servers serve reads from memory when data is available locally
- **Network fallback**: Only when data isn't available locally does the system use network operations

**Zero-Copy Data Access**: Because database components run within application processes, data can often be accessed without serialization overhead or network protocol translation. Keys and values can be passed by reference rather than copied across process boundaries.

**Intelligent Batching**: Multiple transactions are batched together for commit processing, but the embedded deployment means coordination overhead is minimized. When Commit Proxies run on the same nodes as Transaction Builders, batching operations happen through local message passing rather than network protocols.

**Adaptive Coordination**: The system pipelines operations to maximize throughput, but embedded deployment makes coordination more efficient:
- Read versions assigned through local communication when Sequencers are co-located
- Storage Servers can serve reads during write processing without network overhead  
- Conflict resolution benefits from reduced latency between Resolvers and Commit Proxies

This aims to create a performance model where **common operations feel local while maintaining distributed system guarantees**—an approach that differs from traditional client-server database designs.

## Resilience Through Embedded Coordination

Bedrock's approach to failure handling reflects its embedded nature—when your database runs within your application, recovery can be coordinated more tightly with application lifecycle, and failure detection can be more immediate and precise.

### Coordinated Application-Database Recovery

**Component Failures** in embedded deployments create different opportunities and challenges than traditional database architectures:

- **Local Component Failures**: When Storage Servers or Transaction Builders fail within an application process, the Erlang/Elixir supervision tree can restart them immediately, often faster than network-based health checks could detect the failure.
- **Cross-Node Failures**: When entire application nodes fail, the remaining nodes can quickly detect the absence and coordinate both application-level and database-level recovery.
- **Network Partitions**: Raft consensus in the Coordinators ensures that minority partitions halt operations rather than risking split-brain scenarios, but the embedded deployment means that applications and their local database components fail together, reducing complex partial failure scenarios.

**Recovery Patterns** benefit from the tight integration between application and database:

- **Unified Supervision Trees**: Database components participate in the same supervision hierarchies as application logic, enabling coordinated restarts and shared recovery strategies.
- **Application-Aware Recovery**: The recovery process can take advantage of application-specific knowledge about data importance, access patterns, and acceptable recovery trade-offs.
- **Fast Local Recovery**: Components that run embedded can restart in milliseconds rather than the seconds required for network-based service discovery and reconnection.

### Comprehensive Validation

The testing and validation strategy reflects Bedrock's embedded nature:

**Embedded Testing Advantages**: Because database components run within application processes, integration testing can exercise the full application-database stack without complex test environment setup. Property-based tests can validate MVCC and conflict resolution correctness using the same interfaces that application code uses.

**System Transaction as Integration Test**: The Director's configuration persistence serves as a comprehensive end-to-end system validation—if the system can successfully commit its own configuration, the entire transaction pipeline is working correctly. This provides continuous validation that traditional databases can't match.

## Bedrock's Current State and Future Direction

### What Bedrock Provides Today

Bedrock represents an implementation of embedded distributed database concepts that builds on established distributed systems research while exploring new approaches enabled by embedded deployment:

- **Transaction Processing**: Multi-phase commit pipeline with ACID guarantees, optimistic concurrency control, and strong consistency across distributed nodes.
- **Local Read Optimization**: When data is available locally, reads can happen with memory access latency rather than network latency, potentially changing application performance characteristics.
- **Unified Application-Database Lifecycle**: Database components participate in the same supervision trees, configuration management, and deployment processes as application code.
- **Self-Bootstrapping Architecture**: The system can serve as its own configuration database, reducing external dependencies while providing end-to-end system validation.

### Future Development

Bedrock's development continues to explore what's possible when databases and applications are more tightly integrated:

- **Performance Optimization**: Continued work on optimizing the transaction pipeline for both throughput and latency, exploring how embedded deployment might enable performance characteristics not available in traditional architectures.
- **Data Placement Algorithms**: Research into algorithms for automatically placing frequently accessed data on nodes where it's most needed, potentially reducing network traffic and improving response times.
- **Application-Aware Operations**: Exploring deeper integration between application logic and database operations, investigating optimizations based on application-specific knowledge of data access patterns and consistency requirements.

---

## Navigation Guide

### For New Developers
1. **Start Here**: [User's Perspective](../quick-reads/users-perspective.md) - How to interact with Bedrock
2. **Core Concepts**: [Transaction Basics](../quick-reads/transactions.md) - Quick reference for MVCC and versioning
3. **System Layout**: [Transaction System Layout](../quick-reads/transaction-system-layout.md) - Component relationships

### For Implementation Work  
1. **Component References**: [Individual Components](../components/) - Detailed API and implementation docs
2. **Transaction Flow**: [Transaction Deep Dive](transactions.md) - Complete processing lifecycle
3. **Recovery Process**: [Recovery Deep Dive](recovery.md) - Multi-phase recovery system

### For System Understanding
1. **This Document**: Comprehensive architectural patterns and design principles
2. **Specialized Topics**: [Recovery Subsystem](recovery/) - Detailed recovery phase documentation
3. **Reference**: [Glossary](../glossary.md) - Definitions and cross-linked concepts

---

**Footnotes:**  
¹ [FoundationDB Paper](https://www.foundationdb.org/files/fdb-paper.pdf) - The seminal paper on FoundationDB's architecture and design principles

---

## Conclusion: Exploring Database Architecture

Bedrock represents an exploration of **embedded distributed systems**. It attempts to combine the operational simplicity that has made SQLite successful, the performance optimizations demonstrated by DuckDB, and the distributed architecture principles established by FoundationDB—in a single system that runs inside your applications.

This approach represents a different way of thinking about the relationship between applications and data:

- **Simplified deployment**: No separate database servers to manage, while providing ACID transactions and strong consistency across multiple machines
- **Local-first performance**: Memory-speed local reads when possible, with automatic scaling across your cluster when needed
- **Unified lifecycle management**: Database components participate in your application's supervision trees and lifecycle, while maintaining isolation and consistency guarantees

### What This Might Mean for Applications

The embedded distributed approach explores new architectural possibilities:

**Local performance with global consistency**: Applications can potentially access frequently used data at memory speed while maintaining ACID guarantees across a distributed cluster, reducing the traditional trade-off between consistency and performance.

**Simplified operations**: Scaling your application can automatically scale your database. Deploying your application deploys your database. Monitoring your application monitors your database. This reduces the operational complexity of managing separate systems.

**Integrated management**: Database failures, recovery, and operational tasks can coordinate through the same supervision trees and management systems as your application logic, potentially reducing the complexity of managing separate database and application operational models.

### The Path Forward

Bedrock explores whether embedded and distributed approaches can be complementary. By attempting to keep data close to computation while maintaining distributed system guarantees, this architectural approach investigates whether applications can achieve both improved performance and resilience compared to traditional architectures.

The embedded distributed database approach isn't just about performance improvements or operational simplification—it's an investigation into whether new categories of applications might become practical by reducing the overhead and complexity of traditional database architectures.

This architectural foundation represents one approach to making such possibilities more accessible.
