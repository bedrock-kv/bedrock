# FoundationDB Concepts in Bedrock

This document explains how FoundationDB concepts are implemented in Bedrock, based on the architectural design.

## See Also
- **Transaction Details**: [Transaction Lifecycle](transaction-lifecycle.md) - Detailed transaction flow implementation
- **Persistent State**: [Persistent Configuration](persistent-configuration.md) - Self-bootstrapping cluster state design
- **Implementation Guides**: [Control Plane Components](../03-implementation/control-plane-components.md) and [Data Plane Components](../03-implementation/data-plane-components.md)
- **Development Support**: [Best Practices](../02-development/best-practices.md) and [Debugging Strategies](../02-development/debugging-strategies.md)
- **Complete Reference**: [Bedrock Architecture Livebook](../../docs/bedrock-architecture.livemd) - Comprehensive architectural overview

## Core Architecture Principles

Bedrock follows FoundationDB's separation of concerns with specialized components:

### Control Plane vs Data Plane

**Control Plane** (Management and Coordination):
- **Coordinators**: Use Raft consensus to store cluster configuration and elect a Director
- **Director**: Manages system recovery, monitors health, and coordinates the data plane
- **Rate Keeper**: Manages system load and flow control (planned)

**Data Plane** (Transaction Processing):
- **Sequencer**: Assigns global version numbers to transactions
- **Commit Proxies**: Batch transactions and coordinate commits
- **Resolvers**: Implement MVCC conflict detection
- **Logs**: Provide durable transaction storage
- **Storage**: Serve data and follow transaction logs

## Transaction Lifecycle

### 1. Transaction Initiation
```elixir
# Client requests a read version from the Sequencer
{:ok, read_version} = Sequencer.get_read_version()

# Gateway provides a lease for this read version
Gateway.lease_read_version(read_version, timeout)
```

### 2. Reading Data
- All reads in a transaction use the same read version
- Reads of written keys return the pending write value
- Storage servers serve data at the requested version

### 3. Writing Data
- Writes are collected locally until commit
- No network traffic for writes until commit time
- Read-your-writes consistency within the transaction

### 4. Commit Process
```elixir
# Transaction submitted to Commit Proxy
CommitProxy.commit_transaction(%{
  read_version: read_version,
  reads: [keys_and_ranges_read],
  writes: %{key => value, ...}
})
```

**Commit Proxy Workflow**:
1. **Batching**: Collect transactions for a few milliseconds or until batch size limit
2. **Version Assignment**: Get next commit version from Sequencer
3. **Conflict Resolution**: Send batch to appropriate Resolvers
4. **Conflict Detection**: Resolvers check for conflicts and return abort list
5. **Logging**: Non-conflicting transactions sent to Log servers
6. **Acknowledgment**: Wait for Log durability, then notify clients

## Multi-Version Concurrency Control (MVCC)

### Version Management
- **Read Version**: Snapshot version for transaction reads
- **Commit Version**: Global version assigned at commit time
- **Last Committed Version (LCV)**: Highest committed version
- **Minimum Read Version (MRV)**: Oldest version still needed

### Conflict Resolution
Resolvers maintain a sliding window of recent transaction activity:

```elixir
# Resolver checks for conflicts
conflicts = Resolver.check_conflicts(batch, %{
  read_keys: transaction.reads,
  write_keys: Map.keys(transaction.writes),
  read_version: transaction.read_version,
  commit_version: batch.commit_version
})
```

**Conflict Rules**:
- Read-Write conflict: Transaction read a key that was written by a later-committed transaction
- Write-Write conflict: Two transactions wrote to the same key
- Within-batch conflicts: Transactions in the same batch conflict with each other

## Key Distribution and Sharding

### Key Space Organization
- Single, continuous, sorted key-value space
- Keys can be binary or structured (tuples, lists)
- Automatic lexicographic ordering

### Range-Based Sharding
- Key space divided into ranges
- Each range assigned to specific servers
- Dynamic range splitting and merging (planned)

```elixir
# Example key ranges
ranges = [
  {"", "m"},           # Range 1: keys starting with a-l
  {"m", "z"},          # Range 2: keys starting with m-y  
  {"z", :end}          # Range 3: keys starting with z and beyond
]
```

## Durability and Replication

### Transaction Logging
- All committed transactions written to multiple Log servers
- Logs provide durability guarantee before client notification
- Storage servers pull from logs to update their data

### Replication Strategy
- Each key range replicated across multiple Storage servers
- Log servers replicated for durability
- Automatic failover and recovery (in development)

## System Recovery

### Cold Start Process
1. **Coordinator Election**: Raft elects a leader Coordinator
2. **Director Startup**: Leader starts the Director process
3. **Service Discovery**: Director discovers available nodes and services
4. **Range Assignment**: Director assigns key ranges to Storage servers
5. **Log Recovery**: Replay any uncommitted transactions
6. **Service Coordination**: Start Commit Proxies, Resolvers, etc.

### Failure Recovery
- **Node Failure**: Director detects and reassigns services
- **Service Failure**: Automatic restart and state recovery
- **Network Partition**: Raft ensures consistency during splits

## Performance Optimizations

### Batching
- Commit Proxies batch multiple transactions
- Reduces per-transaction overhead
- Improves throughput while maintaining latency

### Pipelining
- Multiple transaction phases can overlap
- Read versions assigned while previous transactions commit
- Storage servers can serve reads while processing writes

### Local Storage Utilization
- Storage servers use local disk/memory
- Reduces network latency for local reads
- Enables horizontal scaling of read capacity

## Comparison to FoundationDB

| Concept | FoundationDB | Bedrock Implementation |
|---------|--------------|----------------------|
| Coordinators | Cluster coordination | `Bedrock.ControlPlane.Coordinator` |
| Cluster Controller | System management | `Bedrock.ControlPlane.Director` |
| Proxies | Transaction processing | `Bedrock.DataPlane.CommitProxy` |
| Resolvers | Conflict detection | `Bedrock.DataPlane.Resolver` |
| Logs | Transaction durability | `Bedrock.DataPlane.Log.Shale` |
| Storage Servers | Data serving | `Bedrock.DataPlane.Storage.Basalt` |
| Sequencer | Version assignment | `Bedrock.DataPlane.Sequencer` |

## Development Status

### Working Components
- Basic Raft consensus (via `bedrock_raft`)
- Component structure and interfaces
- Transaction data structures

### In Development
- Complete transaction flow integration
- Recovery process implementation
- Multi-node coordination
- Performance optimization

### Planned Features
- Automatic sharding and rebalancing
- Advanced failure recovery
- Deterministic simulation testing
- Performance monitoring and tuning
