# Transaction System Layout

**The coordination blueprint that enables distributed transaction processing.**

After recovery brings individual components online, they need to know who handles what. The Transaction System Layout (TSL) provides this coordination map—without it, the sequencer can't notify logs, proxies can't route to resolvers, and storage teams operate in isolation.

## Core Function

The TSL maps every component relationship needed for transaction processing. Think of it as the distributed system's phone book and organizational chart combined.

## Essential Components

### Process References

- **Sequencer**: Global transaction ordering authority
- **Commit Proxies**: Transaction entry points and batching coordinators  
- **Resolvers**: MVCC conflict detection by key range

### System Topology

- **Logs**: Transaction persistence with shard-to-server mappings
- **Storage Teams**: Data storage with key range assignments
- **Services**: Complete worker registry with operational status

### Coordination Metadata

- **Layout ID**: Unique identifier for this system configuration
- **Epoch**: Recovery generation for change tracking
- **Director**: Current cluster coordination authority

## When Created

TSL construction happens during recovery phase 12—after all components are operational but before they can coordinate transactions. The layout is immediately persisted as the authoritative system configuration.

## Quick Reference

```elixir
# View current layout
Bedrock.ControlPlane.Director.get_layout()

# Layout structure
%TransactionSystemLayout{
  id: uuid,
  epoch: 5,
  director: #PID<0.123.45>,
  sequencer: #PID<0.124.46>,
  proxies: [#PID<0.125.47>, ...],
  resolvers: %{key_range => #PID<...>},
  logs: %{shard => #PID<...>}, 
  storage_teams: %{key_range => [#PID<...>]}
}
```

## Implementation

- **Main Module**: `lib/bedrock/control_plane/config/transaction_system_layout.ex`
- **Creation Phase**: `lib/bedrock/control_plane/director/recovery/transaction_system_layout_phase.ex`

## See Also

- [Transaction System Layout Phase](recovery/transaction-system-layout.md) - Detailed creation process
- [Recovery](recovery.md) - Recovery phase context  
- [Transactions](transactions.md) - How TSL enables transaction coordination
