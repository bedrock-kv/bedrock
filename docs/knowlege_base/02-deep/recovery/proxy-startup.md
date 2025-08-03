# Proxy Startup Phase

**Starting commit proxy components for horizontal transaction processing scalability.**

The `ProxyStartupPhase` starts commit proxy components that provide horizontal scalability for transaction processing. Multiple proxies can handle increasing transaction volume without creating bottlenecks at individual components.

## Horizontal Scalability Design

Unlike the sequencer which must be a singleton for consistency, commit proxies can be horizontally scaled to handle increased transaction throughput. Multiple proxies work together to:

- Batch transactions for efficient processing
- Distribute transaction load across the cluster
- Provide fault tolerance through redundancy
- Enable parallel conflict resolution and commitment

## Distribution Strategy

The phase uses round-robin distribution to start the desired number of commit proxy processes across nodes with coordination capabilities from `context.node_capabilities.coordination`. This ensures fault tolerance by spreading proxies across different machines rather than concentrating them on adjacent nodes.

Round-robin distribution provides:
- Geographic fault tolerance across physical machines
- Load balancing across available coordination nodes
- Resilience against node-level failures
- Optimal resource utilization

## Configuration and Coordination

Each proxy is configured with the current epoch and director information, enabling them to:
- Detect epoch changes and coordinate with the recovery system
- Recognize when recovery has moved to a new generation
- Properly handle transaction coordination with other components
- Maintain consistency during system transitions

## Operational Readiness

Importantly, proxies remain locked during startup until the Transaction System Layout phase transitions them to operational mode using their lock tokens. This prevents premature transaction processing before the complete system is ready and all components can coordinate properly.

## Error Conditions

The phase stalls if no coordination-capable nodes are available, since at least one commit proxy must be operational for the cluster to accept transactions. Without commit proxies, the system cannot:
- Accept new transactions from clients
- Coordinate conflict resolution with resolvers
- Manage transaction batching and ordering
- Provide the scalability layer for transaction processing

## Input Parameters

- `context.cluster_config.parameters.desired_commit_proxies` - Target number of commit proxy processes
- `context.node_capabilities.coordination` - Nodes capable of running coordination components including commit proxies
- `recovery_attempt.epoch` - Current recovery epoch for coordination
- `context.lock_token` - Lock token for proxy startup coordination

## Output Results

- `recovery_attempt.proxies` - List of started commit proxy PIDs ready for transaction coordination

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/proxy_startup_phase.ex`

For detailed component behavior, batching strategies, and transaction coordination mechanisms, see [Commit Proxy documentation](../components/commit-proxy.md).

## Next Phase

Proceeds to [Resolver Startup](11-resolver-startup.md) to start MVCC conflict detection components.