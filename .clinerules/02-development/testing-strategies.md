# Testing Strategies for Bedrock

This guide covers testing approaches for distributed systems development in Bedrock, including ideas for implementing FoundationDB-style deterministic simulation.

## Testing Philosophy

Distributed systems require multiple layers of testing:

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test component interactions
3. **System Tests**: Test complete distributed scenarios
4. **Property-Based Tests**: Test invariants and edge cases
5. **Deterministic Simulation**: Test rare failure scenarios

## Current Testing Infrastructure

### Unit Testing

Bedrock uses ExUnit for standard unit testing:

```elixir
# Example unit test
defmodule Bedrock.DataPlane.ResolverTest do
  use ExUnit.Case
  
  test "detects read-write conflicts" do
    # Test conflict detection logic in isolation
  end
end
```

### Property-Based Testing

Using StreamData for property-based testing:

```elixir
# Example property test
property "conflict detection is consistent" do
  check all transactions <- list_of(transaction_generator()) do
    # Test that conflict detection produces consistent results
  end
end
```

### Integration Testing

Testing component interactions:

```elixir
# Example integration test
test "commit proxy coordinates with resolver" do
  # Start commit proxy and resolver
  # Send batch to commit proxy
  # Verify resolver receives batch
  # Check conflict resolution results
end
```

## Multi-Node Testing with bedrock_ex

### Test Harness Setup

The `bedrock_ex` project provides multi-node testing:

```bash
# Start 3-node cluster for testing
cd ../bedrock_ex
iex --name c1@127.0.0.1 -S mix run  # Terminal 1
iex --name c2@127.0.0.1 -S mix run  # Terminal 2
iex --name c3@127.0.0.1 -S mix run  # Terminal 3
```

### Automated Multi-Node Tests

```elixir
# Example multi-node test setup
defmodule Bedrock.ClusterTest do
  use ExUnit.Case
  
  setup do
    # Start multiple nodes
    # Wait for cluster formation
    # Return cluster references
  end
  
  test "cluster handles node failure" do
    # Simulate node failure
    # Verify cluster continues operating
    # Check data consistency
  end
end
```

## Deterministic Simulation Testing

### FoundationDB Inspiration

FoundationDB uses deterministic simulation to find rare bugs by:
- Controlling all sources of non-determinism
- Using a deterministic random number generator
- Simulating network failures, disk failures, etc.
- Running thousands of simulated years in minutes

### Elixir/Erlang Challenges

Implementing deterministic simulation in Elixir faces challenges:

1. **Process Scheduling**: BEAM scheduler is not deterministic
2. **Message Ordering**: Message delivery order can vary
3. **Timing**: Real-time operations introduce non-determinism
4. **External Dependencies**: Network, disk I/O are inherently non-deterministic

### Potential Approaches

#### Approach 1: Controlled Environment

```elixir
defmodule Bedrock.Simulation do
  @moduledoc """
  Deterministic simulation framework for Bedrock testing.
  """
  
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
  
  def add_node(sim, node_id, config) do
    # Add a simulated node with controlled behavior
  end
  
  def simulate_network_partition(sim, nodes_a, nodes_b) do
    # Simulate network partition between node groups
  end
  
  def advance_time(sim, duration) do
    # Advance simulated time and process events
  end
end
```

#### Approach 2: Mock-Based Testing

```elixir
defmodule Bedrock.Test.MockNetwork do
  @moduledoc """
  Mock network layer for deterministic testing.
  """
  
  def send_message(from, to, message, opts \\ []) do
    # Deterministically route messages
    # Can simulate delays, failures, reordering
  end
  
  def simulate_partition(nodes_a, nodes_b) do
    # Block messages between node groups
  end
end
```

#### Approach 3: Event-Driven Simulation

```elixir
defmodule Bedrock.Test.EventSimulator do
  @moduledoc """
  Event-driven simulation for testing distributed scenarios.
  """
  
  defstruct [
    :current_time,
    :event_queue,
    :random_state,
    :network_state,
    :node_states
  ]
  
  def schedule_event(sim, time, event) do
    # Add event to priority queue
  end
  
  def process_next_event(sim) do
    # Process next event in chronological order
  end
  
  def run_simulation(sim, duration) do
    # Run simulation for specified duration
  end
end
```

### Implementation Strategy

1. **Start Simple**: Begin with controlled unit tests
2. **Add Network Simulation**: Mock network layer for deterministic message passing
3. **Time Control**: Replace real time with simulated time
4. **Failure Injection**: Add systematic failure injection
5. **Property Verification**: Check system invariants throughout simulation

### Example Simulation Test

```elixir
defmodule Bedrock.SimulationTest do
  use ExUnit.Case
  
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
    
    # Advance time and verify majority partition continues
    sim = Simulation.advance_time(sim, 30_000)
    assert Simulation.majority_partition_operational?(sim)
    
    # Heal partition
    sim = Simulation.heal_partition(sim)
    sim = Simulation.advance_time(sim, 30_000)
    
    # Verify full cluster recovery
    assert Simulation.cluster_healthy?(sim)
    assert Simulation.data_consistent?(sim)
  end
end
```

## Testing Distributed System Components

### Fake Server Pattern for Component Testing

When testing components that interact with GenServer-based services, create lightweight fake server processes that can respond to calls with controlled behavior. This approach enables testing of complex interaction patterns without requiring full service implementations.

**Key Principles:**
- Use a single response function that receives the actual GenServer call tuple
- Let the response function pattern match on call structure to return appropriate responses
- Keep fake servers alive with recursive loops to handle multiple sequential calls
- Always include catch-all patterns for unexpected calls

**Benefits:**
- **Maximum Flexibility**: Response functions can handle any call pattern by examining the full call structure
- **Real Protocol Testing**: Tests use the actual message formats and call patterns of the system
- **DRY Implementation**: Single helper function works for any GenServer-like component
- **Pattern Matching Power**: Leverage Elixir's pattern matching to create sophisticated response behaviors

**When to Use:**
- Testing sequential fallback logic across multiple services
- Validating version comparison and selection algorithms
- Testing error handling and recovery scenarios
- Simulating various service availability patterns

This pattern is particularly valuable for testing coordinator bootstrap logic, storage discovery, and other distributed coordination scenarios where multiple services must be queried in sequence.

### Implementation Separation for Testability

Separate complex logic from GenServer concerns by creating dedicated implementation modules. This pattern enables comprehensive unit testing of business logic without the overhead of GenServer lifecycle management.

**Structure:**
- Main module provides public API and delegates to implementation
- Implementation module contains pure business logic
- GenServer module handles only process lifecycle and message routing

**Benefits:**
- Business logic can be tested in isolation with simple function calls
- Complex algorithms become easier to reason about and debug
- Implementation details are separated from process management concerns
- Enables property-based testing of core algorithms

## Testing Patterns

### Transaction Testing

```elixir
# Test transaction properties
property "transactions are serializable" do
  check all transactions <- list_of(transaction_generator(), min_length: 2) do
    # Execute transactions concurrently
    # Verify results are equivalent to some serial execution
  end
end

# Test conflict detection
test "resolver detects all conflicts" do
  # Create known conflicting transactions
  # Verify resolver identifies conflicts correctly
end
```

### Recovery Testing

```elixir
# Test system recovery
test "system recovers from coordinator failure" do
  # Start cluster
  # Kill coordinator
  # Verify new coordinator elected
  # Check system continues operating
end

# Test data durability
test "committed transactions survive node failures" do
  # Commit transactions
  # Kill nodes
  # Restart nodes
  # Verify data is still available
end
```

### Performance Testing

```elixir
# Benchmark transaction throughput
test "measures transaction throughput" do
  # Start cluster
  # Generate load
  # Measure transactions per second
  # Verify performance meets requirements
end
```

## Test Data Generation

### Transaction Generators

```elixir
defmodule Bedrock.Test.Generators do
  use ExUnitProperties
  
  def transaction_generator do
    gen all read_keys <- list_of(key_generator()),
            write_pairs <- list_of({key_generator(), value_generator()}) do
      %Transaction{
        reads: read_keys,
        writes: Map.new(write_pairs)
      }
    end
  end
  
  def key_generator do
    string(:alphanumeric, min_length: 1, max_length: 20)
  end
  
  def value_generator do
    binary(min_length: 0, max_length: 1000)
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
      {:sequencer_failure, :sequencer},
      {:multiple_failures, [:node1, :node2]}
    ]
  end
end
```

## Continuous Integration

### Test Organization

```bash
# Run different test suites
mix test --only unit          # Unit tests only
mix test --only integration   # Integration tests
mix test --only simulation    # Simulation tests
mix test --only property      # Property-based tests
```

### Performance Regression Testing

```elixir
# Benchmark tests to catch performance regressions
defmodule Bedrock.BenchmarkTest do
  use ExUnit.Case
  
  @tag :benchmark
  test "transaction throughput benchmark" do
    {time, _result} = :timer.tc(fn ->
      # Run transaction workload
    end)
    
    # Assert performance is within acceptable range
    assert time < 1_000_000  # 1 second
  end
end
```

## Future Testing Enhancements

### Deterministic Simulation Goals

1. **Full Determinism**: Control all sources of non-determinism
2. **Failure Injection**: Systematic testing of failure scenarios
3. **Time Travel**: Ability to replay and debug specific scenarios
4. **Invariant Checking**: Continuous verification of system properties

### Chaos Engineering

```elixir
# Chaos testing framework
defmodule Bedrock.ChaosTest do
  def random_failure_injection(cluster, duration) do
    # Randomly inject failures during operation
    # Network partitions, node crashes, disk failures
    # Verify system maintains correctness
  end
end
```

### Visualization and Debugging

```elixir
# Test result visualization
defmodule Bedrock.Test.Visualizer do
  def generate_timeline(test_results) do
    # Generate timeline of events during test
    # Show message passing, state changes, failures
  end
  
  def export_trace(test_run) do
    # Export detailed trace for debugging
  end
end
```

## Best Practices

1. **Test Isolation**: Each test should be independent
2. **Deterministic Setup**: Use fixed seeds for reproducible tests
3. **Comprehensive Coverage**: Test normal and failure scenarios
4. **Performance Awareness**: Monitor test performance and resource usage
5. **Documentation**: Document test scenarios and expected behaviors

## Tools and Libraries

- **ExUnit**: Standard Elixir testing framework
- **StreamData**: Property-based testing
- **Mox**: Mocking library for testing
- **Benchee**: Performance benchmarking
- **Observer**: Runtime system monitoring
- **Telemetry**: Event-based monitoring and testing
