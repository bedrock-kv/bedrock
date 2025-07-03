# Persistent Configuration Architecture

This document describes Bedrock's approach to persistent cluster configuration, enabling the system to survive cold starts and maintain cluster state across restarts.

## Overview

Bedrock uses a self-bootstrapping persistence strategy where:
- **Coordinators** read cluster state from local storage on startup
- **Director** persists cluster state after successful recovery
- **System** uses its own storage infrastructure for persistence

## Core Design Principles

### 1. Ephemeral Raft + Storage Bootstrap

**Coordinator Raft Logs**: Remain ephemeral (in-memory only)
**Persistent State**: Stored in the system's own key-value storage
**Bootstrap Process**: Coordinators read from storage to initialize Raft state

**Benefits**:
- Raft consensus ensures most recent coordinator wins
- No complex Raft log persistence required
- Leverages existing storage infrastructure
- Natural conflict resolution during startup

### 2. System Transaction as Comprehensive Test

**Dual Purpose**: The director's system transaction serves as both:
- **Persistence mechanism**: Saves cluster state to storage
- **System test**: Validates entire transaction pipeline

**Fail-Fast Recovery**: If system transaction fails, director exits and coordinator retries with fresh director

**Components Tested**:
- Sequencer (version assignment)
- Commit Proxy (batching and coordination)
- Resolver (conflict detection)
- Logs (durable persistence)
- Storage (eventual application)

### 3. Self-Healing Architecture

**Automatic Recovery**: Failed system transactions trigger director restart
**Convergence**: System eventually reaches stable, persistent state
**No Partial States**: Either fully operational or clearly failed

## Bootstrap Flow

### Cold Start (No Existing Storage)

```
1. Coordinator starts
2. Queries foreman for local storage workers
3. No storage found → uses default configuration
4. Raft election with version 0
5. Director starts recovery process
6. Recovery completes → system transaction persists state
7. System ready with persistent configuration
```

### Warm Start (Existing Storage)

```
1. Coordinator starts
2. Queries foreman for local storage workers
3. Reads \xff/system/config from storage
4. Initializes Raft with storage version
5. Raft election (highest storage version wins)
6. System ready (no recovery needed if already operational)
```

### Recovery After Failure

```
1. System transaction fails during recovery
2. Director exits with failure reason
3. Coordinator detects director failure
4. Coordinator starts fresh director (new epoch)
5. Recovery process repeats
6. Eventually succeeds or fails obviously
```

## System Keyspace Layout

### Reserved Key Prefix
All system keys use the prefix `\xff/system/` to separate them from user data.

### Key Layout
```
\xff/system/config          -> {epoch, sanitized_cluster_config}
\xff/system/epoch           -> current_epoch_number  
\xff/system/last_recovery   -> recovery_timestamp_ms
```

### Future Extensions
```
\xff/system/layout/sequencer     -> sequencer_assignment
\xff/system/layout/commit_proxies -> commit_proxy_list
\xff/system/layout/resolvers     -> resolver_assignments
\xff/system/nodes/{node_id}      -> node_capabilities_and_status
```

## Implementation Details

### Coordinator Bootstrap

**Storage Discovery**:
```elixir
# Use foreman to find local storage workers
case Foreman.wait_for_healthy(foreman, timeout: 5_000) do
  :ok ->
    {:ok, workers} = Foreman.storage_workers(foreman)
    read_config_from_storage(workers)
  {:error, :unavailable} ->
    {0, default_config()}  # Fallback to defaults
end
```

**Config Reading**:
```elixir
# Read system configuration from storage
case Storage.fetch(storage_worker, "\xff/system/config", :latest) do
  {:ok, bert_data} -> 
    {version, config} = :erlang.binary_to_term(bert_data)
    {version, config}
  {:error, :not_found} -> 
    {0, default_config()}
end
```

### Director Persistence

**System Transaction Building**:
```elixir
system_transaction = %{
  reads: [],  # System writes don't need reads
  writes: %{
    "\xff/system/config" => :erlang.term_to_binary({epoch, sanitized_config}),
    "\xff/system/epoch" => :erlang.term_to_binary(epoch),
    "\xff/system/last_recovery" => :erlang.term_to_binary(System.system_time(:millisecond))
  }
}
```

**Direct Commit Proxy Submission**:
```elixir
# Director has direct access to commit proxies it just started
case get_available_commit_proxy(director_state) do
  {:ok, commit_proxy} ->
    case CommitProxy.commit(commit_proxy, system_transaction) do
      {:ok, version} -> 
        # Success - system fully operational
        {:ok, version}
      {:error, reason} ->
        # Fail fast - exit and let coordinator retry
        exit({:recovery_system_test_failed, reason})
    end
end
```

### Config Sanitization

**Problem**: Cluster config contains PIDs, refs, and other non-serializable data
**Solution**: Sanitize config before BERT encoding

```elixir
defp sanitize_config_for_persistence(config) do
  config
  |> remove_pids_and_refs()
  |> remove_ephemeral_state()
  |> keep_only_persistent_fields()
end
```

## Error Handling and Edge Cases

### Storage Unavailable During Bootstrap
- **Scenario**: Coordinator starts but storage not ready
- **Handling**: Wait briefly for foreman, then fall back to defaults
- **Result**: System starts with default config, will persist after recovery

### Corrupted Storage Data
- **Scenario**: Storage contains invalid BERT data
- **Handling**: Catch deserialization errors, fall back to defaults
- **Result**: System recovers gracefully, overwrites corrupted data

### System Transaction Failure
- **Scenario**: Any component in transaction pipeline fails
- **Handling**: Director exits immediately with failure reason
- **Result**: Coordinator detects failure and retries with fresh director

### Network Partitions During Bootstrap
- **Scenario**: Coordinator can't reach storage on other nodes
- **Handling**: Only reads from local storage workers
- **Result**: Raft consensus ensures correct coordinator wins

## Testing Strategy

### Unit Tests
- Storage discovery with/without available storage
- Config serialization/deserialization
- Error handling for corrupted data

### Integration Tests
- Cold start scenarios (empty cluster)
- Warm start scenarios (existing storage)
- Failure recovery scenarios (failed system transactions)

### End-to-End Tests
- Full cluster restart cycles
- Network partition recovery
- Mixed scenarios (some nodes with storage, some without)

## Performance Considerations

### Bootstrap Performance
- Storage reads are local (same node)
- Foreman wait timeout should be reasonable (5s)
- BERT serialization is fast for config-sized data

### Recovery Performance
- System transaction tests entire pipeline
- Single transaction validates all components
- Failure detection is immediate

### Storage Overhead
- System keys are small (config size)
- Infrequent writes (only after recovery)
- No impact on normal transaction performance

## Debugging and Monitoring

### Key Metrics
- Bootstrap success/failure rates
- System transaction success/failure rates
- Time to recovery completion
- Storage read/write latencies

### Debugging Commands
```elixir
# Check system configuration
Storage.fetch(storage_worker, "\xff/system/config", :latest)

# Check last recovery time
Storage.fetch(storage_worker, "\xff/system/last_recovery", :latest)

# Monitor director recovery progress
GenServer.call(:bedrock_director, :get_state)
```

### Common Issues
- **Bootstrap fails**: Check foreman health and storage availability
- **System transaction fails**: Check commit proxy, resolver, and log health
- **Config corruption**: Check BERT serialization/deserialization
- **Infinite retries**: Check for persistent system issues

## Future Enhancements

### Fine-Grained Configuration
- Move from single config key to multiple specific keys
- Enable incremental updates
- Improve observability and debugging

### Configuration Versioning
- Add schema versioning for config format
- Enable backward compatibility
- Support config migrations

### External Configuration
- Support reading initial config from external sources
- Enable configuration management integration
- Support configuration validation

## Migration Path

### Phase 1: Basic Persistence (Current Plan)
- Single system config key
- BERT serialization
- Basic error handling

### Phase 2: Enhanced Reliability
- Fine-grained keys
- Better error recovery
- Comprehensive testing

### Phase 3: Advanced Features
- Configuration versioning
- External config sources
- Advanced monitoring
