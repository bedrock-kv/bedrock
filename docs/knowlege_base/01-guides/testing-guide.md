# Bedrock Testing Guide

Comprehensive testing patterns and strategies for distributed systems development in Bedrock.

## See Also
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md)
- **Development Principles**: [Best Practices](../02-development/best-practices.md)
- **Debugging Support**: [Debugging Strategies](../02-development/debugging-strategies.md)
- **Component Testing**: [Control Plane](../03-implementation/control-plane-components.md#testing-control-plane-components) and [Data Plane](../03-implementation/data-plane-components.md#testing-data-plane-components)

## Testing Philosophy

Distributed systems require multiple testing layers:

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test component interactions
3. **System Tests**: Test complete distributed scenarios
4. **Property-Based Tests**: Test invariants and edge cases
5. **Deterministic Simulation**: Test rare failure scenarios

## Core Testing Patterns

### Process Testing - Reliable Synchronization

Use message passing with `assert_receive` instead of delays for deterministic test synchronization:

```elixir
defp spawn_registered_process(name) do
  test_process = self()
  
  pid = spawn(fn ->
    Process.register(self(), name)
    send(test_process, {:registered, self()})
    
    receive do
      :stop -> :ok
    end
  end)
  
  assert_receive {:registered, ^pid}
  on_exit(fn -> send(pid, :stop) end)
  pid
end
```

**Benefits**: Deterministic behavior, faster execution, automatic cleanup.

### Fake Server Pattern for Component Testing

Create lightweight fake server processes for testing complex interaction patterns:

```elixir
defp start_fake_server(response_fun) do
  spawn(fn ->
    receive do
      {:"$gen_call", from, request} ->
        response = response_fun.(request)
        GenServer.reply(from, response)
        fake_server_loop(response_fun)
    end
  end)
end

defp fake_server_loop(response_fun) do
  receive do
    {:"$gen_call", from, request} ->
      response = response_fun.(request)
      GenServer.reply(from, response)
      fake_server_loop(response_fun)
    _ -> fake_server_loop(response_fun)
  end
end
```

**Use Cases**: Testing sequential fallback logic, version comparison, error handling scenarios.

### Configuration Testing - Round-Trip Validation

Test complete encode/decode cycles for serialization logic:

```elixir
test "preserves data through encode/decode cycle" do
  original_config = build_complex_config()
  
  encoded = Persistence.encode_for_storage(original_config, TestCluster)
  decoded = Persistence.decode_from_storage(encoded, TestCluster)
  
  assert decoded.coordinators == original_config.coordinators
  assert decoded.epoch == original_config.epoch
end
```

### Property-Based Testing

Use StreamData for comprehensive property testing:

```elixir
property "conflict detection is consistent" do
  check all transactions <- list_of(transaction_generator()) do
    # Test that conflict detection produces consistent results
    conflicts = Resolver.detect_conflicts(transactions)
    assert valid_conflict_set?(conflicts, transactions)
  end
end

def transaction_generator do
  gen all read_keys <- list_of(key_generator()),
          write_pairs <- list_of({key_generator(), value_generator()}) do
    %Transaction{
      reads: read_keys,
      writes: Map.new(write_pairs)
    }
  end
end
```

## Multi-Node Testing

### Test Harness Setup

Use `bedrock_ex` for multi-node testing:

```bash
# Start 3-node cluster
cd ../bedrock_ex
iex --name c1@127.0.0.1 -S mix run  # Terminal 1
iex --name c2@127.0.0.1 -S mix run  # Terminal 2  
iex --name c3@127.0.0.1 -S mix run  # Terminal 3
```

### Integration Testing Pattern

```elixir
defmodule Bedrock.ClusterTest do
  use ExUnit.Case
  
  setup do
    # Start multiple nodes with controlled configuration
    nodes = start_test_cluster(3)
    on_exit(fn -> cleanup_cluster(nodes) end)
    {:ok, nodes: nodes}
  end
  
  test "cluster handles node failure", %{nodes: nodes} do
    # Verify initial cluster health
    assert cluster_healthy?(nodes)
    
    # Simulate node failure
    [victim | survivors] = nodes
    simulate_node_failure(victim)
    
    # Verify cluster continues operating
    assert majority_operational?(survivors)
    assert data_consistent?(survivors)
  end
end
```

## Deterministic Simulation Framework

### Core Simulation Structure

```elixir
defmodule Bedrock.Simulation do
  defstruct [
    :seed,
    :nodes,
    :network,
    :time,
    :events
  ]
  
  def new(seed) do
    %__MODULE__{
      seed: seed,
      nodes: %{},
      network: NetworkSim.new(seed),
      time: 0,
      events: []
    }
  end
  
  def simulate_network_partition(sim, nodes_a, nodes_b) do
    # Deterministically simulate network partition
    NetworkSim.partition(sim.network, nodes_a, nodes_b)
    |> update_simulation_state(sim)
  end
  
  def advance_time(sim, duration) do
    # Process all events in time window
    Enum.reduce(sim.events, sim, &process_event/2)
  end
end
```

### Simulation Testing Example

```elixir
test "cluster survives network partition" do
  sim = Simulation.new(seed: 12345)
  
  # Setup 5-node cluster
  sim = Enum.reduce(1..5, sim, fn i, acc ->
    Simulation.add_node(acc, "node#{i}", default_config())
  end)
  
  # Wait for cluster formation
  sim = Simulation.advance_time(sim, 10_000)
  assert Simulation.cluster_healthy?(sim)
  
  # Partition network (3 nodes vs 2 nodes)
  sim = Simulation.simulate_partition(sim, ["node1", "node2", "node3"], ["node4", "node5"])
  
  # Verify majority partition continues
  sim = Simulation.advance_time(sim, 30_000)
  assert Simulation.majority_partition_operational?(sim)
  
  # Heal partition and verify recovery
  sim = Simulation.heal_partition(sim)
  sim = Simulation.advance_time(sim, 30_000)
  assert Simulation.cluster_healthy?(sim)
  assert Simulation.data_consistent?(sim)
end
```

## Testing Distributed Components

### System Transaction Testing

Test actual transaction flow for end-to-end validation:

```elixir
test "system transaction validates entire pipeline" do
  # Start all components: commit proxy, resolver, logs
  {:ok, cluster} = start_full_cluster()
  
  # Submit system transaction
  {:ok, result} = submit_system_transaction(cluster, build_config_update())
  
  # Verify end-to-end behavior
  assert result.committed == true
  assert config_persisted?(cluster)
  assert all_nodes_updated?(cluster)
end
```

### Recovery Testing

```elixir
test "system recovers from coordinator failure" do
  cluster = start_cluster_with_coordinator()
  
  # Kill coordinator
  coordinator_pid = get_coordinator_pid(cluster)
  Process.exit(coordinator_pid, :kill)
  
  # Verify new coordinator election
  assert_receive {:new_coordinator, new_pid}, 5_000
  assert new_pid != coordinator_pid
  
  # Check system continues operating
  assert cluster_operational?(cluster)
end
```

## Test Data Generation

### Comprehensive Generators

```elixir
defmodule Bedrock.Test.Generators do
  use ExUnitProperties
  
  def key_generator do
    string(:alphanumeric, min_length: 1, max_length: 20)
  end
  
  def value_generator do
    binary(min_length: 0, max_length: 1000)
  end
  
  def conflicting_transaction_pair do
    gen all key <- key_generator(),
            value1 <- value_generator(),
            value2 <- value_generator() do
      {
        %Transaction{reads: [key], writes: %{key => value1}},
        %Transaction{reads: [key], writes: %{key => value2}}
      }
    end
  end
end
```

### Failure Scenarios

```elixir
defmodule Bedrock.Test.FailureScenarios do
  def network_partition_scenarios do
    [
      {:minority_partition, [1, 2], [3, 4, 5]},
      {:even_split, [1, 2], [3, 4]},
      {:isolated_node, [1], [2, 3, 4, 5]}
    ]
  end
  
  def node_failure_scenarios do
    [
      {:coordinator_failure, :coordinator},
      {:director_failure, :director},
      {:multiple_failures, [:node1, :node2]}
    ]
  end
end
```

## Testing Anti-Patterns

### Avoid These Patterns

❌ **Unreliable Timing**: `Process.sleep(10)` - Use `assert_receive` instead
❌ **Manual Cleanup**: Forgetting cleanup calls - Use `on_exit` instead  
❌ **Incomplete Coverage**: Skipping edge cases - Test systematically
❌ **Mock Heavy**: Over-mocking interactions - Use fake servers for realistic testing

### Best Practices

✅ **Deterministic Synchronization**: Use message passing for coordination
✅ **Automatic Cleanup**: Use `on_exit` for resource management
✅ **Comprehensive Coverage**: Unit → Integration → System → Edge cases
✅ **Real Protocol Testing**: Use actual message formats and call patterns

## Test Organization

### Test Suites

```bash
# Run specific test suites
mix test --only unit          # Unit tests only
mix test --only integration   # Integration tests
mix test --only simulation    # Simulation tests
mix test --only property      # Property-based tests
```

### Performance Testing

```elixir
@tag :benchmark
test "transaction throughput benchmark" do
  {time, _result} = :timer.tc(fn ->
    # Run transaction workload
    process_transaction_batch(1000)
  end)
  
  # Assert performance within acceptable range
  assert time < 1_000_000  # 1 second
end
```

## Implementation Separation for Testability

Separate business logic from GenServer concerns:

```elixir
# Public API module
defmodule Bedrock.Component do
  def complex_operation(params) do
    Bedrock.Component.Impl.complex_operation(params)
  end
end

# Pure implementation module (easily testable)
defmodule Bedrock.Component.Impl do
  def complex_operation(params) do
    # Pure business logic here
    # No GenServer concerns
  end
end

# GenServer wrapper (handles only process lifecycle)
defmodule Bedrock.Component.Server do
  use GenServer
  
  def handle_call(:complex_operation, _from, state) do
    result = Bedrock.Component.Impl.complex_operation(state.params)
    {:reply, result, state}
  end
end
```

## Future Enhancements

### Deterministic Simulation Goals

1. **Full Determinism**: Control all sources of non-determinism
2. **Failure Injection**: Systematic testing of failure scenarios  
3. **Time Travel**: Replay and debug specific scenarios
4. **Invariant Checking**: Continuous verification of system properties

### Chaos Engineering

```elixir
defmodule Bedrock.ChaosTest do
  def random_failure_injection(cluster, duration) do
    # Inject random failures during operation
    # Network partitions, node crashes, disk failures
    # Verify system maintains correctness
    spawn_chaos_monkey(cluster, duration)
  end
end
```

This testing guide provides comprehensive coverage of testing patterns while maintaining practical focus on distributed systems challenges specific to Bedrock development.