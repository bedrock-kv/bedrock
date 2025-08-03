# Transaction System Layout (TSL)

The Transaction System Layout is the blueprint that defines how all components in a Bedrock cluster connect and communicate during transaction processing. It solves the fundamental coordination problem in distributed systems where individual components would otherwise exist in isolation.

## Purpose

Without a TSL, components cannot coordinate:
- The sequencer assigns version numbers but doesn't know which logs to notify
- Commit proxies receive transactions but don't know which resolvers handle conflict detection
- Storage servers need data but don't know which logs contain required transactions
- Resolvers need to detect conflicts but don't know which storage teams to coordinate with

The TSL creates a complete map of component relationships and responsibilities, enabling coordinated transaction processing across the entire cluster.

## Data Structure

The TSL contains these fields that define the complete system topology:

- **id**: Unique layout identifier generated via `TransactionSystemLayout.random_id()`
- **epoch**: Current recovery epoch from the recovery attempt
- **director**: Process ID of the current director (self())
- **sequencer**: Process ID of the validated sequencer
- **rate_keeper**: Set to nil (rate limiting not implemented yet)
- **proxies**: List of validated commit proxy process IDs
- **resolvers**: List of validated resolver `{start_key, process_id}` pairs defining key range responsibilities
- **logs**: Map of log IDs to log descriptors with shard assignments
- **storage_teams**: Storage team configurations with key ranges and replica assignments
- **services**: Service mapping that tracks operational status of all components

See [`TransactionSystemLayoutPhase`](../lib/bedrock/control_plane/director/recovery/transaction_system_layout_phase.ex) for detailed implementation.
