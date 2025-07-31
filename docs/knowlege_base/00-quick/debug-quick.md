# Debug Quick Reference

Essential debugging commands and patterns for Bedrock development.

## Multi-Node Setup

```bash
# Terminal 1: Node c1 (Raft leader)
iex --name c1@127.0.0.1 -S mix run

# Terminal 2: Node c2
iex --name c2@127.0.0.1 -S mix run

# Terminal 3: Node c3
iex --name c3@127.0.0.1 -S mix run
```

## Basic Connectivity

```elixir
# Check node connectivity
Node.list()                    
Node.ping(:c1@127.0.0.1)      # Should return :pong
Node.ping(:c2@127.0.0.1)      
Node.ping(:c3@127.0.0.1)      

# Check processes
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "bedrock"))
```

## Component Status

```elixir
# Coordinator status
Bedrock.ControlPlane.Coordinator.get_leader()
Bedrock.ControlPlane.Coordinator.get_members()

# Director status
Bedrock.ControlPlane.Director.get_status()
Bedrock.ControlPlane.Director.get_assignments()

# Health checks
Bedrock.HealthCheck.check_all()
```

## Git Commands (No Pager)

```bash
git --no-pager diff
git --no-pager log --oneline
git --no-pager status
```

## Test Commands

```bash
# Run specific test
mix test test/path/to/test.exs

# Run with detailed output
mix test --trace

# Run specific test pattern
mix test --grep "recovery"
```

## Debugging Philosophy

1. **Start Simple**: Test single-node before multi-node
2. **Isolate Components**: Debug one component at a time
3. **Follow Data Flow**: Trace through the system systematically
4. **Question Nil Values**: Investigate why unexpected states occur

## Common Issues

- **Node connectivity**: Check `Node.list()` and `Node.ping/1`
- **Process crashes**: Check logs and restart processes
- **Raft issues**: Verify leader election and log replication
- **Recovery failures**: Check Director status and assignments
- **Service discovery race**: Look for "New leader waiting for first consensus" followed by "Leader ready - starting director with N services" logs

## See Also

- **Detailed Debugging**: [Development Deep](../02-deep/development-deep.md)
- **Testing**: [Testing Quick](testing-quick.md)
- **Components**: [Components Quick](components-quick.md)