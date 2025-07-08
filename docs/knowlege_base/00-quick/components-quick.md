# Components Quick Reference

Essential component responsibilities and interactions for Bedrock development.

## Control Plane Components

### Coordinator
- **Purpose**: Raft consensus and Director election
- **Location**: `lib/bedrock/control_plane/coordinator.ex`
- **Key Role**: Maintains cluster configuration, elects single Director

### Director
- **Purpose**: Cluster orchestration and recovery management
- **Location**: `lib/bedrock/control_plane/director.ex`
- **Key Role**: Assigns roles, manages system recovery, monitors health

### Configuration Manager
- **Purpose**: Persistent self-bootstrapping cluster state
- **Location**: `lib/bedrock/control_plane/configuration_manager.ex`
- **Key Role**: Stores cluster config in system's own storage

## Data Plane Components

### Gateway
- **Purpose**: Client interface and transaction coordination
- **Location**: `lib/bedrock/data_plane/gateway.ex`
- **Key Role**: Manages read versions, coordinates transactions

### Sequencer
- **Purpose**: Issues read/commit versions for consistency
- **Location**: `lib/bedrock/data_plane/sequencer.ex`
- **Key Role**: Provides monotonic timestamps for MVCC

### Commit Proxy
- **Purpose**: Batches transactions for conflict resolution
- **Location**: `lib/bedrock/data_plane/commit_proxy.ex`
- **Key Role**: Groups transactions, interfaces with Resolver

### Resolver
- **Purpose**: Detects write-write conflicts
- **Location**: `lib/bedrock/data_plane/resolver.ex`
- **Key Role**: MVCC conflict detection using read/write sets

### Log
- **Purpose**: Durable transaction log storage
- **Location**: `lib/bedrock/data_plane/log.ex`
- **Key Role**: Persists committed transactions

### Storage
- **Purpose**: Key-value storage with version history
- **Location**: `lib/bedrock/data_plane/storage.ex`
- **Key Role**: Manages data and MVCC versions

## Component Interactions

```
Client → Gateway → Sequencer (read version)
Client → Gateway → Storage (reads)
Client → Gateway → Commit Proxy → Resolver → Log → Storage (commits)
Director → All Components (role assignment, health monitoring)
```

## Testing Patterns

- **Unit**: Test individual component behavior in isolation
- **Integration**: Test component pairs and their interactions
- **End-to-End**: Test complete flows through component chain

## See Also

- **Implementation**: [Implementation Guide](../01-guides/implementation-guide.md)
- **Architecture**: [Architecture Guide](../01-guides/architecture-guide.md)
- **Deep Dive**: [Architecture Deep](../02-deep/architecture-deep.md)