# Transaction System Layout Phase

**Recovery phase 12: Creating the coordination blueprint that enables distributed transaction processing.**

After recovery brings components online individually, they exist in isolation—the [sequencer](../../components/control-plane/sequencer.md) can't notify [logs](../../components/data-plane/log.md), [proxies](../../components/control-plane/commit-proxy.md) can't route to [resolvers](../../components/control-plane/resolver.md), and [storage teams](../../components/data-plane/storage.md) operate independently. The Transaction System Layout (TSL) phase solves this coordination problem.

## What It Does

This phase creates the authoritative coordination map that tells every component how to find and communicate with every other component it needs to work with. Think of it as building the distributed system's phone book and organizational chart simultaneously.

The TSL contains:
- Process identifiers for all components
- Key range assignments for storage teams and resolvers  
- Log shard mappings
- Communication endpoints and coordination relationships

## The Process

**Validation**: Quick health check confirms all components remain operational since startup.

**Blueprint Assembly**: Creates the complete [Transaction System Layout](../../quick-reads/transaction-system-layout.md) data structure with all coordination mappings.

**Pipeline Test**: Stores the TSL using a full system transaction that exercises the entire transaction processing pipeline—if any component can't coordinate properly, the transaction fails and recovery terminates.

**Service Unlock Preparation**: Configures all services for coordinated unlocking once the TSL is successfully persisted.

## Critical Safety

The system transaction that persists the TSL serves as the ultimate validation—if components can't coordinate well enough to complete this transaction, they can't handle normal operations. Any failure terminates recovery immediately rather than deploying a broken system.

## Next Steps

Success transitions to [Persistence](persistence.md), which handles the final storage of configuration data and prepares for [Monitoring](monitoring.md) establishment.

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/transaction_system_layout_phase.ex`

## See Also

- [Transaction System Layout](../../quick-reads/transaction-system-layout.md) - The coordination blueprint structure
- [Recovery](../recovery.md) - Complete recovery process overview  
- [Persistence](persistence.md) - Next recovery phase