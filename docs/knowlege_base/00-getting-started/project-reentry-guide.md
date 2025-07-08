# Project Reentry Guide

Welcome back to Bedrock! This guide will help you quickly get back into development after a break.

## See Also
- **Development Setup**: [Development Setup](development-setup.md) - Detailed environment setup and multi-node testing
- **Quick Commands**: [Quick Reference](quick-reference.md) - Common commands and troubleshooting
- **Architecture Overview**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md) - Understanding the system design
- **AI Collaboration**: [AI Assistant Guide](ai-assistant-guide.md) - Effective AI-assisted development
- **Complete Reference**: [Bedrock Architecture Livebook](../../docs/bedrock-architecture.livemd) - Comprehensive architectural overview

## What is Bedrock?

Bedrock is an embedded, distributed key-value store inspired by FoundationDB, designed to scale from ephemeral test instances to multi-node production clusters. It provides ACID guarantees with strict serialization and is built in Elixir.

## Quick Status Check

### What's Currently Implemented
Based on your November 2024 development state:

- **Raft Consensus**: Core consensus protocol via `bedrock_raft` dependency
- **Control Plane**: Coordinator and Director components for cluster management
- **Data Plane**: Basic structure for Commit Proxies, Resolvers, Logs, and Storage
- **Gateway**: Client interface and transaction coordination
- **Test Harness**: `bedrock_ex` project for multi-node testing

### What Needs Work
- **Integration**: Components may not be fully connected
- **Testing**: Limited test coverage for distributed scenarios
- **Recovery**: System recovery processes may be incomplete
- **Performance**: Optimization and benchmarking needed

## Getting Back Into Development

### 1. Environment Setup

```bash
# From the bedrock project root
mix deps.get
mix compile

# Check if basic compilation works
mix test
```

### 2. Test the Multi-Node Setup

The `bedrock_ex` project is your test harness for multi-node development:

```bash
# Terminal 1 - Start first node
cd ../bedrock_ex
iex --name c1@127.0.0.1 -S mix run

# Terminal 2 - Start second node  
cd ../bedrock_ex
iex --name c2@127.0.0.1 -S mix run

# Terminal 3 - Start third node
cd ../bedrock_ex
iex --name c3@127.0.0.1 -S mix run
```

**Expected Behavior**: Nodes should discover each other, form a Raft quorum, elect a leader, and start the recovery process.

### 3. Check What's Working

In any of the IEx sessions, try:

```elixir
# Check if nodes can see each other
Node.list()

# Check cluster status (if implemented)
# This may not work yet - that's expected
```

### 4. Identify Next Steps

Based on what you observe:

- **If nodes don't connect**: Focus on node discovery and clustering
- **If Raft doesn't elect a leader**: Debug the consensus layer
- **If recovery fails**: Work on the Director's recovery process
- **If basic operations don't work**: Focus on the Gateway and transaction flow

## Key Files to Review

### Architecture Documentation
- `docs/bedrock-architecture.livemd` - Your comprehensive architectural overview
- `README.md` - Basic project description and API examples

### Core Components
- `lib/bedrock/cluster.ex` - Main cluster interface
- `lib/bedrock/control_plane/coordinator.ex` - Raft-based coordination
- `lib/bedrock/control_plane/director.ex` - System recovery and management
- `lib/bedrock/cluster/gateway.ex` - Client interface

### Test Harness
- `../bedrock_ex/` - Multi-node testing environment
- `../bedrock_ex/explainer.md` - Test harness documentation (if exists)

## Development Workflow

### Typical Development Cycle
1. **Make changes** in the `bedrock` project
2. **Recompile** with `mix compile`
3. **Test locally** with `mix test`
4. **Test distributed** using `bedrock_ex` multi-node setup
5. **Debug** using IEx and distributed tracing

### Debugging Distributed Issues
- Use `Node.list()` to check connectivity
- Check process supervision trees with `:observer.start()`
- Use telemetry events for tracing (if implemented)
- Log important state transitions

## Next Development Priorities

Based on your goals:

1. **Get basic functionality working**: Single-node key-value operations
2. **Verify multi-node clustering**: Ensure nodes can form a cluster
3. **Implement transaction flow**: Read version → writes → commit
4. **Add comprehensive testing**: Unit and integration tests
5. **Implement deterministic simulation**: FoundationDB-style testing

## Resources

- **Architecture**: `docs/bedrock-architecture.livemd`
- **FoundationDB Papers**: Research the original FDB architecture
- **Development Guides**: `.clinerules/02-development/`
- **Implementation Details**: `.clinerules/03-implementation/`

## Quick Commands Reference

```bash
# Compile and test
mix compile && mix test

# Start single node for development
iex -S mix

# Start multi-node test (from bedrock_ex)
iex --name c1@127.0.0.1 -S mix run

# Check dependencies
mix deps.tree

# Run benchmarks (if any)
mix run benchmarks/transactions/encoding_and_decoding.exs
```

---

**Remember**: This is a complex distributed system. Don't expect everything to work immediately. Focus on one component at a time and build up functionality incrementally.
