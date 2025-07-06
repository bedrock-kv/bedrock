# Recovery Quick Reference

**Essential recovery concepts - optimized for selective loading.**

## When Recovery Triggers
- **Critical Components**: Coordinator, Director, Sequencer, Commit Proxies, Resolvers, Transaction Logs
- **Detection**: `Process.monitor/1` on all transaction components
- **Response**: Immediate director exit → Coordinator retry with incremented epoch

## Recovery Flow
1. **Coordinator Election** (Raft consensus)
2. **Director Startup** (with new epoch)
3. **Service Discovery** (via foreman)
4. **Transaction System Recovery** (sequencer, proxies, resolvers, logs)
5. **System Validation** (system transaction tests entire pipeline)
6. **Continuous Monitoring** (monitor all components)

## Error Handling Philosophy
```elixir
# GOOD: Fail fast
def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
  exit({:component_failure, reason})
end

# BAD: Complex error handling
def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
  # Complex retry logic, partial recovery attempts
end
```

## Key Implementation Points
- **Director monitors ALL transaction components**
- **ANY component failure → Director immediate exit**
- **Coordinator uses simple exponential backoff**
- **No circuit breaker complexity**
- **Epoch-based generation management**

## For Complete Details
- **[Recovery Internals](recovery-internals.md)** - Full recovery philosophy and implementation
- **[Control Plane Components](../03-implementation/control-plane-components.md)** - Director and Coordinator implementation
- **[Best Practices](../02-development/best-practices.md)** - Error handling patterns
