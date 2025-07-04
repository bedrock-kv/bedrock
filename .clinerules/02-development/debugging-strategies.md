# Debugging Strategies for Bedrock

This guide covers practical debugging approaches for distributed systems development in Bedrock.

## See Also
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md) - Understanding the system design
- **Component Details**: [Control Plane Components](../03-implementation/control-plane-components.md) and [Data Plane Components](../03-implementation/data-plane-components.md)
- **Development Principles**: [Best Practices](best-practices.md) - General development guidelines
- **Testing Approaches**: [Testing Strategies](testing-strategies.md) and [Testing Patterns](testing-patterns.md)
- **Quick Commands**: [Quick Reference](../00-start-here/quick-reference.md) - Common debugging commands

## General Debugging Philosophy

Distributed systems are inherently complex. The key to effective debugging is:

1. **Start Simple**: Test single-node scenarios before multi-node
2. **Isolate Components**: Debug one component at a time
3. **Follow the Logical Chain**: Trace symptoms to root causes systematically
4. **Question Assumptions**: Investigate why unexpected states occur (e.g., why is this nil?)
5. **Understand Design Intent**: Learn the architectural purpose before suggesting fixes
6. **Prioritize Stability**: Get system working first, then investigate deeper issues
7. **Use Systematic Approaches**: Follow the data flow through the system
8. **Leverage Observability**: Use logging, tracing, and metrics extensively

## Multi-Node Debugging Setup

### Terminal Organization

When debugging multi-node issues, organize your terminals:

```bash
# Terminal 1: Node c1 (often the Raft leader)
cd ../bedrock_ex
clear; iex --name c1@127.0.0.1 -S mix run

# Terminal 2: Node c2
cd ../bedrock_ex
clear; iex --name c2@127.0.0.1 -S mix run

# Terminal 3: Node c3
cd ../bedrock_ex
clear; iex --name c3@127.0.0.1 -S mix run

# Terminal 4: Development/compilation
cd /path/to/bedrock
# Use this for mix compile, running tests, etc.
```

### Basic Connectivity Checks

Always start with these basic checks:

```elixir
# In any IEx session
Node.list()                    # Should show other nodes
Node.ping(:c1@127.0.0.1)      # Should return :pong
Node.ping(:c2@127.0.0.1)      # Should return :pong
Node.ping(:c3@127.0.0.1)      # Should return :pong

# Check if processes are running
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "bedrock"))
```

### Git Commands for Debugging

When examining code changes, git may use an interactive pager by default. Use the `--no-pager` flag to prevent this:

```bash
# Correct way to view diffs without interactive pager
git --no-pager diff
git --no-pager log --oneline
git --no-pager status

# This will NOT work (wrong flag position)
git diff --no-pager  # ❌ Invalid option
```

## Component-Specific Debugging

### Raft Consensus Issues

When Raft isn't working properly:

```elixir
# Check Raft state (adjust module names as needed)
GenServer.call(:bedrock_coordinator, :get_state)

# Look for Raft-related processes
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "raft"))

# Check if leader election is happening
# Look for log messages about leader election
```

**Common Raft Issues**:
- **No leader elected**: Check if all 3 nodes are running
- **Split brain**: Verify node names and network connectivity
- **Constant re-elections**: Look for timing issues or network instability

### Director Recovery Issues

The Director is responsible for system recovery. Debug with:

```elixir
# Check Director state
GenServer.call(:bedrock_director, :get_state)

# Look for recovery-related processes
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "director"))

# Check if recovery steps are progressing
# Monitor logs for recovery phase messages
```

**Recovery Process Steps** (from your architecture):
1. Determining durable version
2. Creating vacancies
3. Locking available services
4. Defining sequencer, commit proxies, resolvers
5. Filling vacancies
6. Replaying old logs

### Gateway and Transaction Issues

For client-facing issues:

```elixir
# Check Gateway state
GenServer.call(:bedrock_gateway, :get_state)

# Try basic operations (if implemented)
# These may not work yet - that's expected
```

### Service Discovery Issues

Check if services are being discovered and started:

```elixir
# Look for service-related processes
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "foreman"))
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "worker"))

# Check supervision trees
:observer.start()  # Graphical process viewer
```

## Logging and Observability

### Enable Debug Logging

```elixir
# In any IEx session
Logger.configure(level: :debug)

# Or configure in config/dev.exs:
# config :logger, level: :debug
```

### Telemetry Events

Bedrock uses telemetry for observability. Check for events:

```elixir
# List all telemetry events (if implemented)
:telemetry.list_handlers([])

# Attach a handler to see all events
:telemetry.attach_many(
  "debug-handler",
  [
    [:bedrock, :coordinator, :raft, :leader_elected],
    [:bedrock, :director, :recovery, :started],
    [:bedrock, :director, :recovery, :completed]
  ],
  fn event, measurements, metadata, config ->
    IO.inspect({event, measurements, metadata}, label: "TELEMETRY")
  end,
  nil
)
```

### Process Inspection

Use the Observer for visual debugging:

```elixir
:observer.start()
```

Key things to look for:
- **Supervision trees**: Are all expected processes running?
- **Message queues**: Are processes getting overwhelmed?
- **Memory usage**: Any memory leaks?
- **Process crashes**: Red processes indicate crashes

## Common Debugging Scenarios

### Scenario 1: Nodes Don't Connect

**Symptoms**: `Node.list()` returns empty list

**Debug Steps**:
1. Check node names are correct
2. Verify all nodes use same cookie
3. Check network connectivity
4. Look for firewall issues

```elixir
# Check current node name
Node.self()

# Check cookie
Node.get_cookie()

# Try manual connection
Node.connect(:c1@127.0.0.1)
```

### Scenario 2: Raft Leader Not Elected

**Symptoms**: No leader, constant elections

**Debug Steps**:
1. Ensure exactly 3 nodes are running
2. Check for network partitions
3. Look for timing issues
4. Verify Raft configuration

### Scenario 3: Recovery Process Hangs

**Symptoms**: System starts but never becomes operational

**Debug Steps**:
1. Check Director logs for which recovery step is failing
2. Verify service discovery is working
3. Check if required services are available
4. Look for deadlocks or infinite loops

### Scenario 4: Transactions Don't Work

**Symptoms**: Basic key-value operations fail

**Debug Steps**:
1. Check if recovery completed successfully
2. Verify Gateway is operational
3. Check if Sequencer is running
4. Look for missing components in the transaction flow

## Advanced Debugging Techniques

### Distributed Tracing

If implemented, use distributed tracing to follow requests:

```elixir
# Enable tracing (if implemented)
# This would depend on your tracing setup
```

### State Inspection

Dump component states for analysis:

```elixir
# Get detailed state from components
state = GenServer.call(:some_process, :debug_state)
IO.inspect(state, pretty: true, limit: :infinity)
```

### Network Debugging

For network-related issues:

```elixir
# Check network statistics
:net_kernel.nodes_info()

# Monitor network traffic (external tools)
# Use tcpdump, wireshark, or similar tools if needed
```

## Investigation Methodology

### Create Systematic Checklists

Track multiple interconnected issues:

```markdown
### 🔍 Investigation Tasks
- [ ] Examine component X for issue Y
- [ ] Check integration between A and B
- [ ] Verify assumption Z

### 🐛 Known Bugs to Fix
- [ ] Immediate: Fix crash in error handler
- [ ] Root Cause: Investigate why corruption occurs
- [ ] Follow-up: Improve error handling strategy

### 🧪 Testing Strategy
- [ ] Write reproduction test
- [ ] Add defensive handling
- [ ] Verify fix works end-to-end
```

### Distinguish Symptoms from Root Causes

When debugging, always separate what you observe from what's actually broken. For example, an "Enumerable protocol error" might be the symptom, but the root cause could be an error handler returning the wrong data type. Fix the error handler to always return consistent data structures, and the enumerable error disappears. This approach prevents band-aid fixes that mask deeper issues.

### Key Design Patterns for Debugging

**Ephemeral Raft + Storage Bootstrap**: When debugging coordinator issues, remember that Raft logs are ephemeral but coordinators bootstrap from persistent storage. The coordinator with the highest storage version should win elections.

**System Transaction as Comprehensive Test**: Director persistence transactions serve dual purposes - they both persist state AND test the entire data plane pipeline. If this transaction fails, it indicates a systemic issue, not just a persistence problem.

**Fail-Fast Self-Healing**: Components should exit cleanly on unrecoverable errors rather than entering partial states. This triggers automatic retry mechanisms and prevents complex debugging scenarios.

**Leverage Existing Infrastructure**: Before creating special handling, check if existing APIs can be used. For example, use normal transaction flow for system operations rather than creating special transaction types.

## Debugging Checklist

When encountering issues, work through this checklist:

- [ ] All nodes are running and connected
- [ ] Raft has elected a leader
- [ ] Director has started recovery process
- [ ] Required services are discovered and started
- [ ] No obvious errors in logs
- [ ] Process supervision trees look healthy
- [ ] Network connectivity is working
- [ ] No resource exhaustion (memory, CPU, disk)

## Performance Debugging

### Identifying Bottlenecks

```elixir
# Check process message queue lengths
Process.info(pid, :message_queue_len)

# Monitor memory usage
:erlang.memory()

# Check scheduler utilization
:scheduler.utilization(1000)
```

### Profiling

For performance issues:

```elixir
# Use :fprof for function-level profiling
:fprof.start()
# ... run your code ...
:fprof.stop()
:fprof.analyse()

# Use :eprof for process-level profiling
:eprof.start()
# ... run your code ...
:eprof.stop()
:eprof.analyse()
```

## Getting Help

When stuck:

1. **Check the architecture docs**: `docs/bedrock-architecture.livemd`
2. **Review component implementations**: Look at the actual code
3. **Search for similar patterns**: Look at FoundationDB documentation
4. **Create minimal reproduction**: Isolate the issue to the smallest possible case
5. **Document your findings**: Add to this debugging guide for future reference

## Tools and Resources

- **Observer**: `:observer.start()` - Visual process monitoring
- **Debugger**: `:debugger.start()` - Step-through debugging
- **Recon**: External library for production debugging
- **Logger**: Built-in logging with configurable levels
- **Telemetry**: Event-based observability system
