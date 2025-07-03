# Bedrock Quick Reference

This is a quick reference guide for common development tasks and debugging scenarios in Bedrock.

## Getting Started Checklist

- [ ] Read the [Project Reentry Guide](project-reentry-guide.md)
- [ ] Set up development environment using [Development Setup](development-setup.md)
- [ ] Start 3-node cluster with `bedrock_ex`
- [ ] Verify nodes can see each other with `Node.list()`
- [ ] Check if Raft leader is elected
- [ ] Monitor recovery process

## Common Commands

### Development Environment
```bash
# Compile and test
mix deps.get && mix compile && mix test

# Start single node for development
iex -S mix

# Start multi-node cluster (run in separate terminals)
cd ../bedrock_ex
iex --name c1@127.0.0.1 -S mix run
iex --name c2@127.0.0.1 -S mix run
iex --name c3@127.0.0.1 -S mix run
```

### Debugging Commands
```elixir
# Check node connectivity
Node.list()
Node.ping(:c1@127.0.0.1)

# Check running processes
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "bedrock"))

# Start observer
:observer.start()

# Enable debug logging
Logger.configure(level: :debug)

# Check component states
GenServer.call(:bedrock_coordinator, :get_state)
GenServer.call(:bedrock_director, :get_state)
GenServer.call(:bedrock_gateway, :get_state)

# Check persistent configuration
Storage.fetch(storage_worker, "\xff/system/config", :latest)
Storage.fetch(storage_worker, "\xff/system/epoch", :latest)
Storage.fetch(storage_worker, "\xff/system/last_recovery", :latest)

# Check foreman and storage workers
Foreman.storage_workers(foreman)
Foreman.wait_for_healthy(foreman, timeout: 5_000)
```

## Architecture Quick Reference

### Control Plane Components
- **Coordinator**: Raft consensus, leader election, configuration storage
- **Director**: System recovery, service management, health monitoring

### Data Plane Components
- **Sequencer**: Global version assignment
- **Commit Proxy**: Transaction batching and coordination
- **Resolver**: MVCC conflict detection
- **Log (Shale)**: Durable transaction storage
- **Storage (Basalt)**: Data serving and version management

### Transaction Flow
1. **Initiation**: Get read version from Sequencer
2. **Read Phase**: Read data at read version
3. **Write Phase**: Accumulate writes locally
4. **Commit Phase**: Send to Commit Proxy → Resolver → Log
5. **Completion**: Client notification

## Common Issues and Solutions

### Nodes Don't Connect
- Check node names are correct
- Verify same cookie across nodes
- Check network connectivity
- Look for firewall issues

### Raft Leader Not Elected
- Ensure exactly 3 nodes running
- Check for network partitions
- Verify Raft configuration
- Look for timing issues

### Recovery Process Hangs
- Check which recovery phase is failing
- Verify service discovery working
- Check for missing dependencies
- Look for resource constraints

### Transaction Issues
- Verify recovery completed
- Check Gateway operational
- Ensure Sequencer running
- Trace transaction flow

## File Locations Quick Reference

### Key Implementation Files
```
lib/bedrock/cluster.ex                    # Main cluster interface
lib/bedrock/control_plane/coordinator.ex # Raft coordination
lib/bedrock/control_plane/director.ex    # System recovery
lib/bedrock/cluster/gateway.ex           # Client interface
lib/bedrock/data_plane/sequencer.ex      # Version assignment
lib/bedrock/data_plane/commit_proxy.ex   # Transaction batching
lib/bedrock/data_plane/resolver.ex       # Conflict detection
lib/bedrock/data_plane/log/shale.ex      # Transaction logging
lib/bedrock/data_plane/storage/basalt.ex # Data storage
```

### Configuration Files
```
lib/bedrock/control_plane/config.ex      # Main configuration
lib/bedrock/control_plane/config/        # Configuration components
```

### Recovery Implementation
```
lib/bedrock/control_plane/director/recovery/
├── determining_durable_version.ex
├── creating_vacancies.ex
├── locking_available_services.ex
├── defining_sequencer.ex
├── defining_commit_proxies.ex
├── defining_resolvers.ex
├── filling_vacancies.ex
└── replaying_old_logs.ex
```

## Testing Quick Reference

### Running Tests
```bash
mix test                    # All tests
mix test --only unit        # Unit tests only
mix test --only integration # Integration tests
mix test --only property    # Property-based tests
```

### Test Patterns
```elixir
# Unit test example
test "component behavior" do
  # Test individual component logic
end

# Property test example
property "invariant holds" do
  check all input <- generator() do
    # Test property across many inputs
  end
end

# Integration test example
test "components work together" do
  # Test component interactions
end
```

## Performance Monitoring

### Key Metrics to Watch
- Transaction throughput
- Conflict rates
- Recovery time
- Memory usage
- Disk I/O performance

### Monitoring Commands
```elixir
# Memory usage
:erlang.memory()

# Process message queues
Process.info(pid, :message_queue_len)

# Scheduler utilization
:scheduler.utilization(1000)
```

## Documentation Navigation

- **Start Here**: [Project Reentry](project-reentry-guide.md) → [Development Setup](development-setup.md)
- **Architecture**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md) → [Transaction Lifecycle](../01-architecture/transaction-lifecycle.md)
- **Development**: [Debugging Strategies](../02-development/debugging-strategies.md) → [Testing Strategies](../02-development/testing-strategies.md)
- **Implementation**: [Control Plane](../03-implementation/control-plane-components.md) → [Data Plane](../03-implementation/data-plane-components.md)
- **Complete Overview**: [Bedrock Architecture Livebook](../../docs/bedrock-architecture.livemd)

## Next Steps

1. **Get Basic Functionality Working**: Focus on single-node operations first
2. **Verify Multi-Node Clustering**: Ensure nodes form clusters correctly
3. **Implement Transaction Flow**: Complete the read → write → commit cycle
4. **Add Comprehensive Testing**: Build out test coverage
5. **Implement Deterministic Simulation**: Advanced testing for rare scenarios

## Getting Help

When stuck:
1. Check the debugging strategies guide
2. Review component implementation details
3. Look at the comprehensive Livebook documentation
4. Create minimal reproduction cases
5. Document findings for future reference
