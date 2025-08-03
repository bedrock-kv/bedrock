# Transaction System Layout Phase

**Creating the coordination blueprint that enables distributed system components to communicate.**

The `TransactionSystemLayoutPhase` creates the Transaction System Layout (TSL)—the blueprint that enables distributed system components to find and communicate with each other. This phase solves the fundamental coordination problem: without a TSL, operational components exist in isolation, unable to coordinate transaction processing.

## The Coordination Challenge

Recovery has started all the individual components—sequencer, commit proxies, resolvers, logs, and storage servers—but they cannot yet work together. The coordination challenges include:

- The sequencer doesn't know which logs to notify about new versions
- Commit proxies don't know which resolvers handle conflict detection for specific key ranges  
- Storage servers don't know which logs contain the transactions they need
- Resolvers don't know which storage teams to coordinate with for conflict detection

## TSL Construction Process

The TSL construction process validates that core transaction components remain operational, builds the complete coordination blueprint, and prepares services for the system transaction that will persist this configuration. The validation is lightweight since previous phases already established communication with all components—it primarily confirms nothing failed between startup and TSL construction.

The Transaction System Layout contains all the mappings and coordination information necessary for distributed transaction processing:

- Component process IDs and their roles
- Key range assignments for storage teams and resolvers
- Log assignments and shard mappings
- Service coordination relationships
- System topology and communication paths

## Service Unlocking

An important aspect of this phase is preparing services for operational mode. Services that were locked during recruitment and startup are prepared for unlocking, which will occur after the TSL is successfully persisted. This prevents premature transaction processing before the complete system coordination is established.

## Error Conditions

Recovery stalls if validation detects missing components, TSL construction fails, or service unlocking encounters problems, indicating topology issues requiring manual intervention. These conditions suggest fundamental problems with the recovery process that cannot be automatically resolved.

## Input Parameters

Complete set of operational transaction processing components:
- Sequencer PID and status
- Commit proxy PIDs and configurations  
- Resolver PIDs with key range assignments
- Log service mappings and PIDs
- Storage team configurations and service assignments

## Output Results

- Complete Transaction System Layout ready for persistence
- Validated component operational status
- Services prepared for unlocking after persistence

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/transaction_system_layout_phase.ex`

For complete TSL specification including data structure, service mapping, and coordination details, see [Transaction System Layout](../transaction-system-layout.md).

## Next Phase

Proceeds to [Persistence](13-persistence.md) to durably store the system configuration.