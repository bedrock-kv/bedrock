# Control Plane

The Control Plane handles the distributed coordination challenges that make Bedrock a fault-tolerant, consistent distributed database. These components manage cluster state, coordinate recovery operations, and ensure leadership consensus across the distributed system.

## What is the Control Plane?

The Control Plane layer focuses on the "meta" problems of distributed systems: Who is the leader? How do we coordinate recovery when nodes fail? How do we ensure only one authority manages cluster state at a time? These are fundamentally different challenges from transaction processing or data persistence.

Control Plane components deal with:

- **Cluster Coordination**: Maintaining consistent cluster state and service discovery
- **Leadership Management**: Ensuring single points of authority while handling failover
- **Recovery Orchestration**: Coordinating systematic recovery when failures occur
- **Distributed Consensus**: Managing Raft-based consensus for critical cluster decisions

This contrasts with the Data Plane, which handles transaction processing, conflict resolution, and data persistence, and the Gateway layer, which manages client interfaces and request routing.

## Components

### [Coordinator](../deep-dives/architecture/control-plane/coordinator.md)

The foundational authority for cluster state management through Raft distributed consensus. The Coordinator maintains the authoritative service directory, handles service registrations from Gateway nodes, and coordinates Director lifecycle during leadership changes.

**Core Responsibilities:**

- Raft consensus management and leader election
- Service discovery and directory authority  
- Director startup/shutdown coordination
- Cluster configuration persistence

### [Director](../deep-dives/architecture/control-plane/director.md)

The recovery orchestrator that brings the cluster back to operational state after failures. Created by the elected Coordinator leader, the Director coordinates the complex multi-phase recovery process using epoch-based generation management to prevent split-brain scenarios.

**Core Responsibilities:**

- Recovery planning and orchestration
- Epoch-based generation management
- Service lifecycle coordination
- Worker creation requests to Foreman processes

The Control Plane provides the coordination foundation that enables the Data Plane to operate consistently and recover from failures while maintaining ACID transaction guarantees.

## Key Operations Flow

1. **Cluster Startup**: Coordinators establish consensus, elect leader, populate service directory
2. **Service Registration**: Gateways register available services with leader Coordinator  
3. **Recovery Initiation**: Coordinator leader creates Director with current epoch
4. **Recovery Orchestration**: Director coordinates systematic recovery of Data Plane components
5. **Operational State**: Cluster serves transactions while Control Plane monitors health

## Fault Tolerance Strategy

Control Plane components implement specific strategies for distributed fault tolerance:

**Consensus-Based Authority**: Coordinators use Raft consensus to maintain consistent cluster state across node failures and network partitions.

**Epoch-Based Coordination**: Directors use generation counters to prevent split-brain scenarios, ensuring only one Director operates per epoch.

**Service Discovery Resilience**: Service directory information replicates across all Coordinators, eliminating single points of failure for topology information.

**Recovery Rollback**: Multi-phase recovery includes rollback capabilities when component startup fails, maintaining cluster consistency.

## Design Principles

The Control Plane follows these design principles to maintain system reliability:

- **Single Authority**: One elected leader per critical function (Coordinator leader, single Director per epoch)
- **Consensus-Based Decisions**: All critical state changes go through Raft consensus for consistency
- **Graceful Handoff**: Leadership transitions coordinate cleanly without losing cluster state
- **Recovery Orchestration**: Centralized recovery coordination prevents race conditions and partial recovery states

## See Also

- [Data Plane Overview](data-plane.md) - Transaction processing and conflict resolution components coordinated by the Control Plane
- [Infrastructure Components](../deep-dives/architecture/infrastructure/README.md) - Foundational cluster interface and client gateway components  
- [Recovery Deep Dive](../deep-dives/recovery.md) - Detailed examination of Control Plane recovery coordination
- [Cluster Startup](../deep-dives/cluster-startup.md) - Control Plane role in cluster initialization
- [Architecture Overview](../deep-dives/architecture.md) - System-wide architectural context and component relationships
