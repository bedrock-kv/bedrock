# Transaction System Layout

**The coordination blueprint that enables distributed transaction processing.**

After recovery brings individual components online, they need to know who handles what. The Transaction System Layout (TSL) provides this coordination map—without it, the sequencer can't notify logs, proxies can't route to resolvers, and storage teams operate in isolation.

## Core Function

The TSL maps every component relationship needed for transaction processing. Think of it as the distributed system's phone book and organizational chart combined.

## Essential Components

### Process References

- **Sequencer**: Global transaction ordering authority
- **Rate Keeper**: System-wide rate limiting and flow control
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
{:ok, layout} = Bedrock.ControlPlane.Director.fetch_transaction_system_layout(director_pid)

# Layout structure
%TransactionSystemLayout{
  id: 42,
  epoch: 5,
  director: #PID<0.123.45>,
  sequencer: #PID<0.124.46>,
  rate_keeper: #PID<0.126.48>,
  proxies: [#PID<0.125.47>, #PID<0.127.49>],
  resolvers: [
    %{start_key: "", resolver: #PID<0.128.50>},
    %{start_key: "m", resolver: #PID<0.129.51>}
  ],
  logs: %{0 => log_descriptor, 1 => log_descriptor},
  storage_teams: [storage_team_descriptor, ...]
}
```

## Implementation

- **Main Module**: `lib/bedrock/control_plane/config/transaction_system_layout.ex`
- **Creation Phase**: `lib/bedrock/control_plane/director/recovery/transaction_system_layout_phase.ex`

## See Also

- [Transaction System Layout Phase](recovery/transaction-system-layout.md) - Detailed creation process
- [Recovery](recovery.md) - Recovery phase context  
- [Transactions](transactions.md) - How TSL enables transaction coordination
