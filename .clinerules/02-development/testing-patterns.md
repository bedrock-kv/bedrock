# Testing Patterns for Bedrock

This document covers specific testing patterns and techniques discovered during Bedrock development. For general testing philosophy and strategies, see [Testing Strategies](testing-strategies.md). For broader development principles, see [Best Practices](best-practices.md).

## Process Testing Patterns

### Reliable Process Synchronization

**Reference**: See [Best Practices - Code as Source of Truth](best-practices.md#code-as-source-of-truth-principle) for the principle of verifying actual behavior.

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

**Key Benefits**:
- Deterministic behavior (no race conditions)
- Faster execution than delays
- Automatic cleanup via `on_exit`

### Test Helper DRY Principle

**Reference**: See [Best Practices - DRY Implementation](best-practices.md#dry-dont-repeat-yourself-implementation) for the general DRY principle.

Extract repetitive test setup into reusable helper functions. When you see the
same process spawning/registration pattern more than two times, create a helper function.

## Configuration Testing Patterns

### Round-Trip Encoding/Decoding Tests

When implementing serialization logic (like PID → {otp_name, node} conversion), always test the complete round-trip:

```elixir
test "preserves data through encode/decode cycle" do
  original_config = build_complex_config()
  
  encoded = Persistence.encode_for_storage(original_config, TestCluster)
  decoded = Persistence.decode_from_storage(encoded, TestCluster)
  
  # Verify non-serializable data is preserved
  assert decoded.coordinators == original_config.coordinators
  assert decoded.epoch == original_config.epoch
end
```

### Edge Case Coverage

**Reference**: See [Best Practices - Test Edge Cases Thoroughly](best-practices.md#test-edge-cases-thoroughly) for comprehensive edge case testing.

Always test:
- Non-existent processes (should return `nil`)
- Empty collections
- Mixed valid/invalid data
- Network partitions (local vs remote processes)

## Integration Testing Patterns

### System Transaction Testing

For components that interact with the transaction system, test the actual transaction flow rather than mocking:

```elixir
test "system transaction validates entire pipeline" do
  # Use real commit proxy, resolver, logs
  # Submit actual system transaction
  # Verify end-to-end behavior
end
```

**Reference**: See [Persistent Configuration Architecture](../01-architecture/persistent-configuration.md#system-transaction-as-comprehensive-test) for the dual-purpose design.

## Test Organization Principles

### Testing is Non-Negotiable

**Reference**: See [Best Practices - Documentation and Knowledge Sharing](best-practices.md#documentation-and-knowledge-sharing) for the importance of documenting decisions.

Testing steps should never be removed from implementation plans. Always implement:
- Unit tests for complex logic
- Integration tests for component interactions  
- Error handling tests for failure scenarios
- Performance tests for critical paths

### Batch Test Improvements

**Reference**: See [Best Practices - Batch Simple Changes](best-practices.md#batch-simple-changes) for efficient change management.

When refactoring tests, make comprehensive changes in single passes rather than incremental updates:
- Remove all manual cleanup calls at once
- Update all variable names consistently
- Apply DRY helpers across all similar tests

## Anti-Patterns to Avoid

### Unreliable Timing

❌ **Don't use delays**: `Process.sleep(10)` is unreliable and slow
✅ **Use message synchronization**: `assert_receive {:registered, ^pid}`

### Manual Resource Management

❌ **Don't manually clean up**: Forgetting cleanup calls leads to test pollution
✅ **Use automatic cleanup**: `on_exit(fn -> cleanup() end)`

### Incomplete Test Coverage

❌ **Don't skip testing**: "It's simple, doesn't need tests"
✅ **Test systematically**: Unit → Integration → System → Edge cases

## Cross-References

- **General Principles**: [Best Practices](best-practices.md)
- **Testing Philosophy**: [Testing Strategies](testing-strategies.md)
- **Debugging Approaches**: [Debugging Strategies](debugging-strategies.md)
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md)
