# Resolver Startup Phase

**Starting MVCC conflict detection components for transaction isolation.**

How does a distributed database prevent conflicting transactions from corrupting data when multiple clients modify overlapping key ranges simultaneously? The `ResolverStartupPhase` solves this critical concurrency control challenge by starting resolver components that implement MVCC (Multi-Version Concurrency Control) conflict detection through a sophisticated multi-step process.

## Transformation Challenge

The phase transforms the abstract resolver descriptors generated during vacancy creation into operational resolver processes that understand exactly which storage teams they coordinate with and which transaction logs contain relevant historical data. This requires complex range-to-storage-team mapping and tag-based log assignment to ensure each resolver gets precisely the data it needs for conflict detection.

## Range Generation and Storage Team Mapping

The phase first converts resolver start keys into concrete key ranges by sorting descriptors and creating adjacent ranges covering the entire keyspace. Each resolver range is then mapped to storage team tags using sophisticated range overlap detection—this determines which storage teams each resolver must coordinate with for conflict detection.

This mapping ensures that:
- Every key in the system has exactly one responsible resolver
- Resolvers understand which storage teams they coordinate with
- Key range boundaries align properly with storage team responsibilities  
- No gaps or overlaps exist in conflict detection coverage

## Tag-Based Log Assignment

Resolvers are assigned logs whose range tags intersect with their storage team tags through a complex filtering algorithm. This ensures each resolver receives transaction data relevant to its key range while avoiding unnecessary data transfer. The algorithm uses tag-based filtering with deduplication to create minimal but complete log sets for each resolver.

Benefits of this targeted log assignment include:
- Reduced network traffic for historical data recovery
- Faster resolver startup through focused data processing
- Optimal memory usage for conflict detection state
- Elimination of irrelevant transaction data

## Node Assignment and Recovery

Resolvers are distributed across resolution-capable nodes using round-robin assignment for fault tolerance. Each resolver starts with a lock token and recovers its historical conflict detection state by processing transactions from assigned logs within the established version vector range—this builds the MVCC tracking information needed to detect future conflicts.

## Historical State Recovery

The historical state recovery process is critical for maintaining MVCC consistency across the recovery boundary. Resolvers must rebuild their conflict detection information by:

- Processing all transactions within the version vector range
- Tracking which keys were accessed by which transactions
- Building version-based conflict detection state
- Preparing to handle new transaction conflicts correctly

Without this historical state recovery, resolvers would not be able to detect conflicts between old transactions (from before recovery) and new transactions (after recovery), potentially allowing data corruption.

## Error Conditions

The phase stalls if no resolution-capable nodes are available or if individual resolver startup fails, since conflict detection is fundamental to transaction isolation guarantees.

## Input Parameters

- `recovery_attempt.resolvers` - Resolver descriptors from vacancy creation with start keys
- `recovery_attempt.storage_teams` - Storage team descriptors with key ranges and tags  
- `recovery_attempt.logs` - Log descriptors with tag assignments
- `recovery_attempt.service_pids` - Running log processes for historical data recovery
- `context.node_capabilities.resolution` - Nodes capable of running resolver components
- `recovery_attempt.version_vector` - Version range for historical conflict state recovery
- `context.lock_token` - Lock token for resolver coordination

## Output Results

- `recovery_attempt.resolvers` - List of `{start_key, resolver_pid}` tuples for operational resolvers ready for conflict detection

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/resolver_startup_phase.ex`

For detailed MVCC algorithms and conflict detection mechanisms, see [Resolver documentation](../components/resolver.md).

## Next Phase

Proceeds to [Transaction System Layout](12-transaction-system-layout.md) to create the coordination blueprint for all components.