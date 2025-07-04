# AI Assistant Guide for Bedrock Development

This guide helps AI assistants (like Claude/Cline) work more effectively with the Bedrock codebase by providing structured context and common patterns.

## Quick Context Loading

When an AI assistant joins a Bedrock development session, they should:

1. **Read this guide first** for immediate context
2. **Check the Project Reentry Guide** for current project status
3. **Review the Quick Reference** for common commands and issues
4. **Understand the architecture** via FoundationDB Concepts

## Project Status Summary

### What's Working (as of last update)
- Basic Elixir project structure with dependencies
- Raft consensus integration via `bedrock_raft`
- Component interfaces and module structure
- Multi-node test harness (`bedrock_ex`)

### What's In Development
- Complete transaction flow integration
- Recovery process implementation
- Multi-node coordination
- Service discovery and management

### What's Planned
- Deterministic simulation testing
- Performance optimization
- Automatic sharding and rebalancing

## Common Development Tasks

### 1. Debugging Multi-Node Issues
```bash
# Start 3-node cluster
cd ../bedrock_ex
# Terminal 1: iex --name c1@127.0.0.1 -S mix run
# Terminal 2: iex --name c2@127.0.0.1 -S mix run  
# Terminal 3: iex --name c3@127.0.0.1 -S mix run

# Check connectivity
Node.list()
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "bedrock"))
```

### 2. Component Development Pattern
When implementing or fixing components:
1. Check the relevant implementation guide (control-plane or data-plane)
2. Look at existing module structure in `lib/bedrock/`
3. Follow the GenServer patterns established in the codebase
4. Add appropriate telemetry events
5. Write tests following the patterns in `test/`

### 3. Architecture Questions
- **Control Plane**: Coordinator (Raft), Director (recovery), Config management
- **Data Plane**: Sequencer (versions), Commit Proxy (batching), Resolver (MVCC), Log (durability), Storage (serving)
- **Transaction Flow**: Read version → reads → writes → commit → conflict resolution → logging

## Key File Patterns

### Module Organization
```
lib/bedrock/
├── cluster.ex                    # Main cluster interface
├── control_plane/               # Coordination and management
│   ├── coordinator.ex           # Raft consensus
│   ├── director.ex              # System recovery
│   └── config.ex                # Configuration management
├── data_plane/                 # Transaction processing
│   ├── sequencer.ex             # Version assignment
│   ├── commit_proxy.ex          # Transaction batching
│   ├── resolver.ex              # Conflict detection
│   ├── log/                     # Durable storage
│   └── storage/                 # Data serving
└── cluster/
    └── gateway.ex               # Client interface
```

### GenServer Patterns
Most components follow this pattern:
- `component.ex` - Public API
- `component/server.ex` - GenServer implementation
- `component/state.ex` - State management
- `component/telemetry.ex` - Observability
- `component/tracing.ex` - Distributed tracing

## Common Code Patterns

### Error Handling
```elixir
# Standard error returns
{:ok, result} | {:error, reason}

# Common error reasons
{:error, :unavailable}
{:error, :timeout}
{:error, :conflict}
```

### Telemetry Events
```elixir
# Event naming pattern
[:bedrock, :component, :operation, :status]

# Example
:telemetry.execute([:bedrock, :director, :recovery, :started], %{}, %{phase: phase})
```

### Configuration Access
```elixir
# Get cluster configuration
{:ok, config} = YourCluster.fetch_config()

# Access node-specific config
node_config = YourCluster.node_config()
```

## Testing Patterns

**Reference**: See [Testing Patterns](../02-development/testing-patterns.md) for detailed testing techniques and [Testing Strategies](../02-development/testing-strategies.md) for overall testing philosophy.

### Key Testing Principles
- Use `assert_receive` for reliable test synchronization (never `Process.sleep`)
- Use `on_exit` for automatic test cleanup
- Extract repetitive test setup into DRY helper functions
- Test round-trip encoding/decoding for serialization logic
- Testing is non-negotiable - always implement comprehensive tests

## Debugging Checklist

When encountering issues:
- [ ] Check if all nodes are connected (`Node.list()`)
- [ ] Verify Raft leader is elected
- [ ] Check Director recovery status
- [ ] Look for process crashes in Observer
- [ ] Check logs for error messages
- [ ] Verify configuration consistency across nodes

## AI Assistant Best Practices

### When Analyzing Code
1. **Start with the public API** in the main module file
2. **Check the server implementation** for GenServer logic
3. **Look at state management** for data structures
4. **Review tests** for expected behavior examples

### When Implementing Features
1. **Follow existing patterns** in similar components
2. **Add appropriate telemetry** for observability
3. **Include error handling** for distributed scenarios
4. **Write tests** at multiple levels (unit, integration, property)
5. **Place imports at module top** - `require`, `alias`, `import` go at the top, never inside functions
6. **Question nil values** - investigate why something is nil before handling it
7. **Understand design intent** - learn the architectural purpose before suggesting changes
8. **Verify code over documentation** - when implementation and docs disagree, code is source of truth
9. **Apply DRY principle** - extract repetitive patterns into generic helper functions
10. **Leverage existing infrastructure** - use established `otp_name` patterns rather than creating new mechanisms

### When Debugging
1. **Use the debugging strategies guide** for systematic approaches
2. **Check component states** with GenServer calls
3. **Monitor telemetry events** for system behavior
4. **Use Observer** for visual process monitoring

### Collaboration Best Practices

#### Planning and Design Phase
1. **Present choices** when multiple approaches are viable - users appreciate having options to evaluate
2. **Engage in back-and-forth** during design phase to refine ideas before implementation
3. **Ask clarifying questions** to understand user preferences and constraints
4. **Document design decisions** in knowledge base before implementing
5. **Reference existing infrastructure** rather than creating new patterns unnecessarily

#### Implementation Approach
1. **Use existing APIs** and interfaces rather than direct GenServer calls for future-proofing
2. **Leverage established patterns** like foreman/worker, fail-fast recovery, and self-healing
3. **Minimize special cases** - prefer normal transaction flow over custom handling
4. **Focus on prose over code** in documentation unless specific examples add value
5. **Reference knowledge base** heavily in implementation plans rather than repeating content

## Context for AI Responses

When providing assistance:
- **Reference specific files** from the codebase structure
- **Use established patterns** from the knowledgebase
- **Consider distributed system challenges** (network partitions, node failures, etc.)
- **Think about FoundationDB concepts** and how they apply
- **Suggest appropriate testing strategies** for the changes

## Quick Architecture Reminders

- **Raft Consensus**: 3-node quorum required for progress
- **Recovery Process**: Multi-phase Director-managed recovery
- **MVCC**: Conflict detection via sliding window in Resolvers
- **Version Management**: Global ordering via Sequencer
- **Transaction Batching**: Commit Proxies batch for efficiency
- **Durability**: Logs provide durability before client notification
- **Persistent Configuration**: Coordinators bootstrap from storage, Director persists via system transaction
- **Self-Healing**: Failed system transactions trigger director restart and coordinator retry

This guide should be updated as the project evolves to maintain accuracy and usefulness.
