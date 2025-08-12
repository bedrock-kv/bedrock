# Bedrock Components

This directory organizes Bedrock's components by their architectural role within the distributed database system. Each component implements specific responsibilities that work together to provide ACID transactions, MVCC concurrency control, and fault tolerance.

## Architecture Overview

Bedrock follows a layered architecture that separates concerns between transaction coordination, data persistence, and storage engine implementations. The **Infrastructure** layer provides foundational cluster interface, client gateway, and worker process management. The **Control Plane** handles transaction coordination, version management, conflict detection, and commit orchestration. The **Data Plane** manages persistent storage interfaces and distributed system coordination. The **Service Layer** provides service abstractions and lifecycle patterns. Finally, **Implementations** are concrete storage engines that implement data plane interfaces with specific performance characteristics. These layers interact with Infrastructure feeding into both Control Plane and Data Plane, Control Plane coordinating with Data Plane, and both feeding through the Service Layer to the concrete Implementations.

## Control Plane Components

These components handle cluster coordination, recovery orchestration, and distributed consensus:

- **[Coordinator](architecture/control-plane/coordinator.md)** - Manages cluster state through Raft consensus and coordinates Director lifecycle
- **[Director](architecture/control-plane/director.md)** - Orchestrates recovery and epoch-based generation management for the transaction system

For a comprehensive overview, see [Control Plane Overview](../quick-reads/control-plane.md).

## Data Plane Components

These components handle transaction processing, conflict resolution, and persistent storage interfaces:

- **[Sequencer](architecture/data-plane/sequencer.md)** - The version authority that assigns globally unique, monotonically increasing version numbers for MVCC
- **[Commit Proxy](architecture/data-plane/commit-proxy.md)** - Orchestrates the two-phase commit protocol across log servers and provides horizontal scaling for transaction processing
- **[Resolver](architecture/data-plane/resolver.md)** - Detects conflicts between concurrent transactions using version analysis
- **[Log](architecture/data-plane/log.md)** - Persistent transaction log interface that maintains committed transaction history
- **[Storage](architecture/data-plane/storage.md)** - Multi-version key-value storage interface that serves read operations

For a comprehensive overview, see [Data Plane Overview](../quick-reads/data-plane.md).

## Implementation Components

These are concrete storage engines that implement the data plane interfaces:

- **[Basalt](architecture/implementations/basalt.md)** - An example multi-version storage engine based on :dets
- **[Shale](architecture/implementations/shale.md)** - An example disk-based write-ahead-log implementation with segment file management

Implementation components can be swapped or configured based on performance requirements, hardware characteristics, and deployment constraints.

## Infrastructure Components

These components provide foundational cluster interface, client gateway, and worker process management:

- **[Cluster](architecture/infrastructure/cluster.md)** - Foundational interface and configuration management for Bedrock distributed database instances
- **[Gateway](architecture/infrastructure/gateway.md)** - Client-facing interface that handles requests and routes them to appropriate components
- **[Transaction Builder](architecture/infrastructure/transaction-builder.md)** - Constructs and validates transactions from client operations
- **[Foreman](architecture/infrastructure/foreman.md)** - Manages worker processes and service lifecycle operations across cluster nodes

Infrastructure components provide the high-level abstractions and client interfaces that applications use to interact with Bedrock clusters.

## Cross-Component Workflows

Bedrock components coordinate through key cross-cutting workflows that span multiple architectural layers:

### Transaction Processing

The core workflow for processing client transactions through the distributed system. Components collaborate to provide ACID guarantees, MVCC consistency, and horizontal scalability.

For detailed workflow documentation, see **[Transaction Processing Deep Dive](./transactions.md)**.

### Recovery Coordination

Systematic recovery from system failures through multi-phase orchestration. Components participate in service locking, state reconstruction, and system layout rebuilding to restore full operational capability.

For detailed workflow documentation, see **[Recovery Deep Dive](./recovery.md)**.

### Cluster Startup

Initial bootstrap process for new Bedrock clusters. Components coordinate to establish consensus leadership, initialize system configuration, and create the first operational transaction system layout.

For detailed workflow documentation, see **[Cluster Startup Deep Dive](./cluster-startup.md)**.

This separation ensures system-wide consistency and reliability while allowing each component to optimize for its specific responsibilities.
