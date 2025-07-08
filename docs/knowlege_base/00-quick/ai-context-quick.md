# AI Quick Context for Bedrock

**Essential context for AI assistants - optimized for token efficiency.**

## Core Architecture (30-second overview)
- **Control Plane**: Coordinator (Raft consensus) + Director (recovery management)
- **Data Plane**: Sequencer (versions) + Commit Proxy (batching) + Resolver (MVCC) + Log (durability) + Storage (serving)
- **Recovery Philosophy**: "Let it crash" + fast recovery over complex error handling

## Key Principles
1. **ANY transaction component failure → Director exits → Coordinator retries**
2. **Use `Process.monitor/1` for failure detection, not health checking**
3. **Simple exponential backoff (1s, 2s, 4s, 8s, max 30s)**
4. **Each recovery increments epoch counter**

## Common Patterns
- **Error Handling**: Fail fast with `exit({:reason, details})`
- **Testing**: Use `assert_receive` not `Process.sleep`
- **GenServer Structure**: `component.ex` (API) → `component/server.ex` (GenServer)
- **Cross-References**: Always update "See Also" sections when adding content

## File Locations
- **Control Plane**: `lib/bedrock/control_plane/` (coordinator, director)
- **Data Plane**: `lib/bedrock/data_plane/` (sequencer, commit_proxy, resolver, log, storage)
- **Tests**: `test/bedrock/` (mirrors lib structure)

## Debug Commands
```elixir
Node.list()  # Check connectivity
GenServer.call(:bedrock_coordinator, :get_state)
GenServer.call(:bedrock_director, :get_state)
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "bedrock"))
```

## When You Need More Context
- **Architecture Details**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md)
- **Recovery Deep Dive**: [Recovery Internals](../01-architecture/recovery-internals.md)
- **Implementation Guides**: [Control Plane](../03-implementation/control-plane-components.md) | [Data Plane](../03-implementation/data-plane-components.md)
- **Development Patterns**: [Best Practices](../02-development/best-practices.md)
- **Testing Guidance**: [Testing Patterns](../02-development/testing-patterns.md)
