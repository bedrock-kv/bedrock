# Development Patterns Quick Reference

**Essential patterns for Bedrock development - optimized for selective loading.**

## Error Handling
```elixir
# GOOD: Fail fast for critical components
def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
  exit({:component_failure, reason})
end

# GOOD: Graceful degradation for non-critical
def handle_missing_data(nil), do: default_value()
def handle_missing_data(data), do: data
```

## Testing Patterns
```elixir
# GOOD: Reliable synchronization
assert_receive {:registered, ^pid}

# BAD: Unreliable timing
Process.sleep(10)

# GOOD: Automatic cleanup
on_exit(fn -> cleanup_resources() end)
```

## GenServer Structure
- `component.ex` - Public API
- `component/server.ex` - GenServer implementation  
- `component/impl.ex` - Pure business logic (testable)
- `component/state.ex` - State management

## Recovery Principles
1. **Monitor all transaction components** via `Process.monitor/1`
2. **Exit immediately** on any component failure
3. **Use simple exponential backoff** (1s, 2s, 4s, 8s, max 30s)
4. **No complex circuit breakers** or retry logic

## Common Anti-Patterns
- ❌ Complex error handling in recovery phases
- ❌ Health checking instead of process monitoring
- ❌ Manual resource cleanup in tests
- ❌ Hardcoded assumptions about process availability

## For Complete Details
- **[Best Practices](best-practices.md)** - Comprehensive development guidelines
- **[Testing Patterns](testing-patterns.md)** - Detailed testing techniques
- **[Recovery Internals](../01-architecture/recovery-internals.md)** - Recovery philosophy
