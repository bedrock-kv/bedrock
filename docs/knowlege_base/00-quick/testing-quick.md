# Testing Quick Reference

Essential testing patterns and commands for Bedrock development.

## Testing Layers

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test component interactions  
3. **System Tests**: Test complete distributed scenarios
4. **Property-Based Tests**: Test invariants and edge cases
5. **Deterministic Simulation**: Test rare failure scenarios (planned)

## Test Commands

```bash
# Run all tests
mix test

# Run specific test file
mix test test/path/to/test.exs

# Run with detailed output
mix test --trace

# Run specific test pattern
mix test --grep "recovery"

# Run property-based tests
mix test --grep "property"

# Run tests with coverage
mix test --cover
```

## Testing Patterns

### Unit Test Example
```elixir
defmodule Bedrock.DataPlane.ResolverTest do
  use ExUnit.Case
  
  test "detects read-write conflicts" do
    # Test conflict detection logic in isolation
  end
end
```

### Property-Based Test Example
```elixir
property "conflict detection is consistent" do
  check all transactions <- list_of(transaction_generator()) do
    # Test that conflict detection produces consistent results
  end
end
```

### Integration Test Example
```elixir
test "commit proxy coordinates with resolver" do
  # Start commit proxy and resolver
  # Send batch to commit proxy
  # Verify resolver receives correct data
end
```

## Component-Specific Testing

### Control Plane Components
- **Coordinator**: Test Raft consensus and leader election
- **Director**: Test role assignment and recovery processes
- **Configuration Manager**: Test persistent state management

### Data Plane Components
- **Gateway**: Test client interface and transaction coordination
- **Sequencer**: Test version generation and monotonicity
- **Commit Proxy**: Test batch processing and coordination
- **Resolver**: Test conflict detection algorithms
- **Log**: Test durability and ordering guarantees
- **Storage**: Test MVCC and version management

## Test Data Generation

```elixir
# Transaction generator
def transaction_generator do
  gen all read_set <- list_of(string(:alphanumeric)),
          write_set <- list_of({string(:alphanumeric), term()}),
          do: %Transaction{read_set: read_set, write_set: write_set}
end
```

## Multi-Node Testing

```elixir
# Start multiple nodes for testing
test "multi-node recovery" do
  nodes = [:c1@127.0.0.1, :c2@127.0.0.1, :c3@127.0.0.1]
  # Test distributed behavior
end
```

## Test Structure Best Practices

- **Arrange**: Set up test conditions
- **Act**: Execute the behavior being tested
- **Assert**: Verify expected outcomes
- **Cleanup**: Clean up resources (use `on_exit`)

## See Also

- **Detailed Testing**: [Testing Guide](../01-guides/testing-guide.md)
- **Debugging**: [Debug Quick](debug-quick.md)
- **Components**: [Components Quick](components-quick.md)