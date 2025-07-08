# Bedrock Architecture Guide

A comprehensive guide to Bedrock's distributed key-value store architecture, transaction processing, and core concepts.

## See Also
- **Recovery System**: [Recovery Internals](../01-architecture/recovery-internals.md) - "Let it crash" recovery philosophy
- **Persistent State**: [Persistent Configuration](../01-architecture/persistent-configuration.md) - Self-bootstrapping design
- **Implementation Details**: [Control Plane Components](../03-implementation/control-plane-components.md) and [Data Plane Components](../03-implementation/data-plane-components.md)
- **Development Support**: [Best Practices](../02-development/best-practices.md) and [Debugging Strategies](../02-development/debugging-strategies.md)
- **Complete Reference**: [Bedrock Architecture Livebook](../../docs/bedrock-architecture.livemd)

## Core Architecture Principles

Bedrock follows FoundationDB's separation of concerns with specialized components across two planes:

### Control Plane (Management and Coordination)
- **Coordinators**: Use Raft consensus for cluster configuration and Director election
- **Director**: Manages system recovery, health monitoring, and data plane coordination
- **Rate Keeper**: System load and flow control management (planned)

### Data Plane (Transaction Processing)
- **Sequencer**: Assigns global version numbers to transactions
- **Commit Proxies**: Batch transactions and coordinate commits
- **Resolvers**: Implement MVCC conflict detection
- **Logs**: Provide durable transaction storage
- **Storage**: Serve data and follow transaction logs

## Complete Transaction Lifecycle

### 1. Transaction Initiation
```elixir
# Client initiates transaction
Repo.transaction(fn repo ->
  # Operations go here
end)

# Under the hood
{:ok, read_version} = Gateway.get_read_version()
Gateway.lease_read_version(read_version, timeout)
```

**What happens:**
- Client contacts Gateway for transaction lease
- Gateway requests read version from Sequencer
- Read version represents consistent database snapshot
- All transaction reads use this version

### 2. Read Phase
```elixir
# Client reads data
value = Repo.fetch(repo, "customer/123")

# Gateway tracks reads
{:ok, value} = Gateway.fetch(read_version, "customer/123")
```

**Read behavior:**
- All reads use transaction's read version
- Gateway tracks keys and ranges read
- Storage servers provide version-specific data
- Read-your-writes within transaction

### 3. Write Phase
```elixir
# Client accumulates writes
Repo.put(repo, "customer/123/balance", 500)

# Gateway buffers locally
Gateway.put("customer/123/balance", 500)
```

**Write behavior:**
- Writes buffered locally until commit
- No network traffic for writes until commit
- Client can read its own writes immediately

### 4. Commit Phase
```elixir
# Transaction commits (implicit)
CommitProxy.commit({
  {read_version, read_keys_and_ranges},  # reads
  write_key_values                       # writes
})
```

**Commit workflow:**
1. **Batching**: Commit Proxy batches multiple transactions
2. **Version Assignment**: Get next commit version from Sequencer
3. **Conflict Resolution**: Send batch to appropriate Resolvers
4. **Conflict Detection**: Check for read-write, write-write, and within-batch conflicts
5. **Logging**: Non-conflicting transactions sent to Log servers
6. **Acknowledgment**: Wait for Log durability, then notify clients

### 5. Completion
```elixir
# Success case
{:ok, result} = Repo.transaction(fn repo -> result end)

# Conflict case
{:error, :conflict} = Repo.transaction(fn repo -> operations end)
```

## Multi-Version Concurrency Control (MVCC)

### Version Management
- **Read Version**: Snapshot version for transaction reads
- **Commit Version**: Global version assigned at commit time
- **Last Committed Version (LCV)**: Highest committed version
- **Minimum Read Version (MRV)**: Oldest version still needed

### Conflict Resolution
```elixir
# Resolver checks for conflicts
conflicts = Resolver.check_conflicts(batch, %{
  read_keys: transaction.reads,
  write_keys: Map.keys(transaction.writes),
  read_version: transaction.read_version,
  commit_version: batch.commit_version
})
```

**Conflict types:**
- **Read-Write**: Transaction read key written by later-committed transaction
- **Write-Write**: Two transactions wrote same key
- **Within-batch**: Transactions in same batch conflict

## Key Distribution and Sharding

### Key Space Organization
- Single, continuous, sorted key-value space
- Binary or structured keys (tuples, lists)
- Automatic lexicographic ordering

### Range-Based Sharding
```elixir
# Key ranges distributed across storage servers
ranges = [
  {"", "m"},           # Range 1: keys a-l
  {"m", "z"},          # Range 2: keys m-y  
  {"z", :end}          # Range 3: keys z and beyond
]
```

## System Recovery and Durability

### Cold Start Process
1. **Coordinator Election**: Raft elects leader Coordinator
2. **Director Startup**: Leader starts Director process
3. **Service Discovery**: Director discovers available nodes
4. **Range Assignment**: Director assigns key ranges to Storage servers
5. **Log Recovery**: Replay uncommitted transactions
6. **Service Coordination**: Start Commit Proxies, Resolvers, etc.

### Durability Guarantees
- All committed transactions written to multiple Log servers
- Logs provide durability before client notification
- Storage servers pull from logs to update data
- Each key range replicated across multiple Storage servers

## Transaction Guarantees

Bedrock provides full ACID guarantees:
- **Atomicity**: All writes commit or none do
- **Consistency**: Database moves between valid states
- **Isolation**: Strict serialization (transactions appear sequential)
- **Durability**: Committed transactions survive node failures

## Performance Optimizations

### Batching Strategy
- Commit Proxies batch multiple transactions
- Reduces per-transaction overhead
- Improves throughput while maintaining latency

### Pipelining
- Multiple transaction phases overlap
- Read versions assigned while previous transactions commit
- Storage servers serve reads while processing writes

### Local Storage Utilization
- Storage servers use local disk/memory
- Reduces network latency for local reads
- Enables horizontal scaling of read capacity

## Component Mapping

| Concept | FoundationDB | Bedrock Implementation |
|---------|--------------|----------------------|
| Coordinators | Cluster coordination | `Bedrock.ControlPlane.Coordinator` |
| Cluster Controller | System management | `Bedrock.ControlPlane.Director` |
| Proxies | Transaction processing | `Bedrock.DataPlane.CommitProxy` |
| Resolvers | Conflict detection | `Bedrock.DataPlane.Resolver` |
| Logs | Transaction durability | `Bedrock.DataPlane.Log.Shale` |
| Storage Servers | Data serving | `Bedrock.DataPlane.Storage.Basalt` |
| Sequencer | Version assignment | `Bedrock.DataPlane.Sequencer` |

## Common Issues and Debugging

### Transaction Conflicts
- High conflict rates: Check for hot keys or poor transaction design
- Retry with exponential backoff on conflicts
- Consider transaction granularity and access patterns

### Performance Issues
- Slow commits: Check Log server performance and durability settings
- Read version delays: Check Sequencer performance and load
- Hot ranges: Monitor key distribution and consider splitting

### System Recovery
- Node failures: Director detects and reassigns services automatically
- Network partitions: Raft ensures consistency during splits
- Log corruption: Recovery process replays from last known good state

## Development Status

### ✅ Completed
- Basic Raft consensus via `bedrock_raft`
- Component structure and interfaces
- Transaction data structures and basic flow

### 🚧 In Development
- Complete transaction flow integration
- Multi-node coordination and failure handling
- Performance optimization and tuning

### 📋 Planned
- Automatic sharding and rebalancing
- Deterministic simulation testing
- Advanced monitoring and observability

This architecture guide provides the foundation for understanding Bedrock's design and implementation. For detailed implementation guidance, see the component-specific guides in the implementation section.