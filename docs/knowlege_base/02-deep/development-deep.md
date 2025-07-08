# Development Deep Dive: Comprehensive Reference for Bedrock

This document serves as the definitive development reference for Bedrock, combining detailed content from best practices, debugging strategies, and development tools. It provides complete coverage of development practices, debugging techniques, and essential tools for effective Bedrock development.

## Table of Contents

1. [Development Philosophy and Principles](#development-philosophy-and-principles)
2. [Code Style and Organization](#code-style-and-organization)
3. [Testing Strategies and Patterns](#testing-strategies-and-patterns)
4. [Debugging Methodologies](#debugging-methodologies)
5. [Multi-Node Development](#multi-node-development)
6. [Component-Specific Debugging](#component-specific-debugging)
7. [Performance Analysis](#performance-analysis)
8. [Development Tools](#development-tools)
9. [Workflow Optimization](#workflow-optimization)
10. [Architecture Decision Making](#architecture-decision-making)
11. [Common Pitfalls and Solutions](#common-pitfalls-and-solutions)

## See Also

- **Testing Approaches**: [Testing Strategies](../02-development/testing-strategies.md) and [Testing Patterns](../02-development/testing-patterns.md) - Comprehensive testing guidance
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md) - Understanding the system design
- **Recovery Philosophy**: [Recovery Internals](../01-architecture/recovery-internals.md) - "Let it crash" recovery principles
- **Implementation Guides**: [Control Plane Components](../03-implementation/control-plane-components.md) and [Data Plane Components](../03-implementation/data-plane-components.md)
- **AI Collaboration**: [AI Assistant Guide](../00-start-here/ai-assistant-guide.md) - Effective AI-assisted development patterns
- **Quick Commands**: [Quick Reference](../00-start-here/quick-reference.md) - Common debugging commands

## Development Philosophy and Principles

### General Debugging Philosophy

Distributed systems are inherently complex. The key to effective debugging is:

1. **Start Simple**: Test single-node scenarios before multi-node
2. **Isolate Components**: Debug one component at a time
3. **Follow the Logical Chain**: Trace symptoms to root causes systematically
4. **Question Assumptions**: Investigate why unexpected states occur (e.g., why is this nil?)
5. **Understand Design Intent**: Learn the architectural purpose before suggesting fixes
6. **Prioritize Stability**: Get system working first, then investigate deeper issues
7. **Use Systematic Approaches**: Follow the data flow through the system
8. **Leverage Observability**: Use logging, tracing, and metrics extensively

### Core Development Principles

**Code as Source of Truth**: When documentation and implementation disagree, the code is the source of truth. Always verify actual implementation before making assumptions based on documentation.

**Fail-Fast Self-Healing**: Components should exit cleanly on unrecoverable errors rather than entering partial states. This triggers automatic retry mechanisms and prevents complex debugging scenarios.

**Leverage Existing Infrastructure**: Before creating special handling, check if existing APIs can be used. For example, use normal transaction flow for system operations rather than creating special transaction types.

## Code Style and Organization

### Code as Source of Truth Principle

**Rule**: When documentation and implementation disagree, the code is the source of truth.

**Application**: Always verify actual implementation before making assumptions based on documentation. For example, when implementing persistent configuration, the actual `CommitProxy.commit/2` function signature (tuple format) took precedence over documentation examples (map format).

**Benefits**: Prevents implementation bugs, ensures consistency, reduces debugging time.

### DRY (Don't Repeat Yourself) Implementation

**Rule**: Eliminate repetitive code patterns through generic helper functions.

**Pattern**: When you see similar code repeated 3+ times, extract it into a reusable function.

**Example**: Instead of separate `encode_director_reference/2`, `encode_sequencer_reference/2`, etc., create `encode_single_reference/3` that handles all single PID references generically.

**Benefits**: Reduces maintenance burden, improves consistency, makes changes easier to implement across the codebase.

### Leverage Existing Infrastructure

**Rule**: Before creating new mechanisms, thoroughly investigate existing patterns and infrastructure.

**Application**: Use established `otp_name` patterns for process references rather than inventing new naming schemes. Build upon existing behaviours and contracts.

**Benefits**: Consistency with existing patterns, automatic compatibility with new types, reduced maintenance burden.

### Separate Implementation from GenServer Concerns

When building GenServer-based components, separate complex business logic from process lifecycle management by creating dedicated implementation modules.

**Structure Pattern:**
- Main module provides public API and delegates to implementation
- Implementation module (`impl.ex`) contains pure business logic functions
- GenServer module handles only process lifecycle, state management, and message routing

**Benefits:**
- Business logic can be unit tested without GenServer overhead
- Complex algorithms become easier to reason about and debug
- Implementation details are cleanly separated from process concerns
- Enables comprehensive testing of edge cases and error conditions

### Use Long-Form Aliases

Prefer explicit, long-form aliases over grouped imports for better readability and maintainability. Write each alias on its own line to make dependencies more visible and improve code navigation.

### Follow Elixir Style Guide

Avoid `is_` prefixes for predicate functions. Use question marks for boolean-returning functions. Let the formatter handle spacing and indentation consistently.

## Testing Strategies and Patterns

### Key Testing Principles

- **Testing is non-negotiable** - always implement comprehensive tests
- **Use reliable synchronization patterns** (`assert_receive` vs delays)
- **Apply DRY principle** to test helpers and setup code
- **Test edge cases thoroughly** (nil values, empty collections, error conditions)
- **Structure tests at multiple levels** (unit, integration, system)

### Testing Workflow

Follow a systematic testing approach:

1. **Unit tests first** for the specific module being changed
2. **Integration tests** for interactions with other components
3. **Full test suite** to ensure no regressions across the system

This layered approach catches issues at the appropriate level of granularity.

## Debugging Methodologies

### Investigation Methodology

#### Create Systematic Checklists

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

#### Distinguish Symptoms from Root Causes

When debugging, always separate what you observe from what's actually broken. For example, an "Enumerable protocol error" might be the symptom, but the root cause could be an error handler returning the wrong data type. Fix the error handler to always return consistent data structures, and the enumerable error disappears. This approach prevents band-aid fixes that mask deeper issues.

#### Key Design Patterns for Debugging

**Ephemeral Raft + Storage Bootstrap**: When debugging coordinator issues, remember that Raft logs are ephemeral but coordinators bootstrap from persistent storage. The coordinator with the highest storage version should win elections.

**System Transaction as Comprehensive Test**: Director persistence transactions serve dual purposes - they both persist state AND test the entire data plane pipeline. If this transaction fails, it indicates a systemic issue, not just a persistence problem.

**Fail-Fast Self-Healing**: Components should exit cleanly on unrecoverable errors rather than entering partial states. This triggers automatic retry mechanisms and prevents complex debugging scenarios.

**Leverage Existing Infrastructure**: Before creating special handling, check if existing APIs can be used. For example, use normal transaction flow for system operations rather than creating special transaction types.

### Debugging Checklist

When encountering issues, work through this checklist:

- [ ] All nodes are running and connected
- [ ] Raft has elected a leader
- [ ] Director has started recovery process
- [ ] Required services are discovered and started
- [ ] No obvious errors in logs
- [ ] Process supervision trees look healthy
- [ ] Network connectivity is working
- [ ] No resource exhaustion (memory, CPU, disk)

## Multi-Node Development

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

## Performance Analysis

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

### Performance Considerations

#### Avoid Unnecessary Network Calls

When possible, use local data rather than making network requests. Local manifest data and cached information can often provide the same information without the overhead and latency of network calls. Reserve network operations for when they're truly necessary.

#### Batch Operations When Possible

Group related operations to reduce overhead. Single-pass operations over collections are more efficient than multiple passes. Consider the data flow through your functions and structure them to minimize redundant work.

## Development Tools

### Coverage Analysis Tools

**coverage_summary.exs** - Analyzes coverage data for specific modules

To verify the test coverage for a module:
```bash
mix coveralls.json && ./scripts/coverage_summary.exs apps/collx/cover/excoveralls.json lib/path/to/module.ex
```

### Coverage Improvement Process

Follow this systematic 5-step approach for consistent coverage improvements:

1. **Observe**: `mix coveralls.json` to produce an overview report of coverage by module
2. **Analyze**: `./scripts/coverage_summary.exs [coverage-file] [module-path]`
3. **Write tests** for uncovered functions, focusing on pattern matching branches and edge cases
4. **Verify**: Run tests and check coverage improvement

### Logging and Observability

#### Enable Debug Logging

```elixir
# In any IEx session
Logger.configure(level: :debug)

# Or configure in config/dev.exs:
# config :logger, level: :debug
```

#### Telemetry Events

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

#### Process Inspection

Use the Observer for visual debugging:

```elixir
:observer.start()
```

Key things to look for:
- **Supervision trees**: Are all expected processes running?
- **Message queues**: Are processes getting overwhelmed?
- **Memory usage**: Any memory leaks?
- **Process crashes**: Red processes indicate crashes

### Advanced Debugging Tools

#### Distributed Tracing

If implemented, use distributed tracing to follow requests:

```elixir
# Enable tracing (if implemented)
# This would depend on your tracing setup
```

#### State Inspection

Dump component states for analysis:

```elixir
# Get detailed state from components
state = GenServer.call(:some_process, :debug_state)
IO.inspect(state, pretty: true, limit: :infinity)
```

#### Network Debugging

For network-related issues:

```elixir
# Check network statistics
:net_kernel.nodes_info()

# Monitor network traffic (external tools)
# Use tcpdump, wireshark, or similar tools if needed
```

### Essential Debug Tools

- **Observer**: `:observer.start()` - Visual process monitoring
- **Debugger**: `:debugger.start()` - Step-through debugging
- **Recon**: External library for production debugging
- **Logger**: Built-in logging with configurable levels
- **Telemetry**: Event-based observability system

## Workflow Optimization

### Batch Simple Changes

When making multiple related changes (like alias replacements), combine them into a single operation rather than making each change separately. This approach reduces API costs for AI-assisted development, requires fewer compilation cycles, creates atomic changes that are easier to review, and reduces context switching between different types of modifications.

### Account for Auto-Formatting

Remember that format-on-save can shift code, affecting diffs. Always use the final formatted content as reference for subsequent changes, run the formatter before making diffs, and be aware that spacing, quotes, and line breaks may change automatically. This prevents confusion when creating search and replace operations.

### Compile Early and Often

Run `mix compile` after each significant change to catch syntax errors immediately. This workflow provides several benefits: catching syntax errors early, verifying dependencies are correct, ensuring changes don't break compilation, and providing a faster feedback loop during development.

### Test Incrementally

Follow a systematic testing approach: run unit tests first for the specific module being changed, then integration tests for interactions with other components, and finally the full test suite to ensure no regressions across the system. This layered approach catches issues at the appropriate level of granularity.

## Architecture Decision Making

### Present Multiple Options

When there are multiple valid approaches, present them with pros and cons to enable informed decision-making. Consider factors like performance requirements, extensibility needs, consistency with existing patterns, and maintenance burden. This collaborative approach helps ensure the best solution is chosen for the specific context.

### Choose Extensible Solutions

Prefer solutions that automatically support new types without code changes. Extensible solutions are future-proof against new requirements, reduce maintenance burden, follow the open/closed principle, and enable plugin-style architectures. For example, using behaviour callbacks rather than hardcoded module matching allows new worker types to work automatically.

### Documentation and Knowledge Sharing

#### Document Decisions and Rationale

When making architectural decisions, document the reasoning behind the choice. Include alternative approaches that were considered and why they were rejected. This context helps future developers understand the trade-offs and makes it easier to revisit decisions when requirements change.

#### Update Knowledge Base

After completing significant work, update the relevant knowledge base sections. Add new patterns to best practices, document common issues and solutions, update implementation guides with lessons learned, and add debugging tips for new components. The knowledge base should be a living resource that grows with the project.

## Common Pitfalls and Solutions

### Common Debugging Scenarios

#### Scenario 1: Nodes Don't Connect

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

#### Scenario 2: Raft Leader Not Elected

**Symptoms**: No leader, constant elections

**Debug Steps**:
1. Ensure exactly 3 nodes are running
2. Check for network partitions
3. Look for timing issues
4. Verify Raft configuration

#### Scenario 3: Recovery Process Hangs

**Symptoms**: System starts but never becomes operational

**Debug Steps**:
1. Check Director logs for which recovery step is failing
2. Verify service discovery is working
3. Check if required services are available
4. Look for deadlocks or infinite loops

#### Scenario 4: Transactions Don't Work

**Symptoms**: Basic key-value operations fail

**Debug Steps**:
1. Check if recovery completed successfully
2. Verify Gateway is operational
3. Check if Sequencer is running
4. Look for missing components in the transaction flow

### Error Handling Patterns

#### Graceful Degradation

Handle missing or malformed data gracefully rather than crashing. Design functions to return sensible defaults when encountering unexpected input, and structure collection operations to safely filter out invalid items. This approach makes the system more robust and easier to debug.

#### Fail-Fast When Appropriate

Some errors should cause immediate failure rather than silent degradation. Programming errors, invalid configuration, and contract violations should fail fast with clear error messages. This prevents subtle bugs from propagating through the system and makes issues easier to diagnose.

### Development Pitfalls to Avoid

#### Don't Optimize Prematurely

Focus on correctness and clarity first. Write code that is easy to understand and maintain, then optimize only when performance becomes a measurable problem. Premature optimization often leads to complex code that is harder to debug and maintain.

#### Don't Ignore Edge Cases

Always consider what happens with unexpected input. Design functions to handle nil values, empty collections, and malformed data gracefully. Edge case handling often reveals important assumptions in the code that may not hold in production environments.

#### Don't Hardcode Assumptions

Make code flexible and configurable rather than embedding assumptions about specific types or values. Use behaviour contracts, configuration parameters, and pattern matching to create code that adapts to different scenarios without modification.

## Getting Help and Resources

When stuck:

1. **Check the architecture docs**: `docs/bedrock-architecture.livemd`
2. **Review component implementations**: Look at the actual code
3. **Search for similar patterns**: Look at FoundationDB documentation
4. **Create minimal reproduction**: Isolate the issue to the smallest possible case
5. **Document your findings**: Add to this debugging guide for future reference

## Summary

This comprehensive development reference provides complete coverage of development practices, debugging techniques, and essential tools for effective Bedrock development. It combines detailed explanations from best practices, debugging strategies, and development tools into a single authoritative resource.

The guide emphasizes systematic approaches to debugging, comprehensive testing strategies, and workflow optimization techniques. It covers everything from basic connectivity checks to advanced distributed system debugging, providing both theoretical understanding and practical commands.

Key takeaways:
- Always start with simple, systematic debugging approaches
- Use comprehensive testing at multiple levels
- Leverage existing infrastructure before creating new patterns
- Document decisions and update knowledge base regularly
- Focus on correctness first, then optimize

This guide should be updated regularly as new patterns and lessons emerge from development work, ensuring it remains a living resource that grows with the project.