# Persistence Phase

**Durably storing system configuration through a complete system transaction.**

The `PersistencePhase` persists cluster configuration through a complete system transaction. It constructs a system transaction containing the full cluster configuration and submits it through the entire data plane pipeline. This simultaneously persists the new configuration and validates that all transaction components work correctly.

## Dual Purpose Design

This phase serves two critical purposes:

1. **Configuration Persistence**: Durably stores the new system configuration for future recovery attempts and coordinator handoffs
2. **System Validation**: Exercises the complete transaction processing pipeline to prove all components work correctly together

## Storage Strategy

The phase stores configuration in both monolithic and decomposed formats to support different access patterns:

### Monolithic Storage
Complete configurations stored under single keys support coordinator handoff scenarios where the entire system state must be transferred between coordinator instances. This enables seamless coordinator transitions without losing system configuration.

### Decomposed Storage  
Individual components stored under separate keys allow targeted component access during normal operations. This enables efficient retrieval of specific configuration aspects without loading the entire system state.

## System Transaction Pipeline

The system transaction exercises every component in the transaction processing pipeline:

1. **Transaction Building**: Configuration data assembled into transaction format
2. **Conflict Resolution**: Resolvers validate no conflicts (should be none for system keys)
3. **Version Assignment**: Sequencer assigns commit version to the configuration transaction
4. **Log Persistence**: Transaction data written to all logs for durability
5. **Storage Commitment**: Configuration data committed to storage servers
6. **Completion Notification**: Transaction success confirmed through entire pipeline

This end-to-end validation proves that the recovered system can handle actual transaction workloads.

## Failure Handling

If the system transaction fails, the director exits immediately rather than retrying. System transaction failure indicates fundamental problems that require coordinator restart with a new epoch. Possible failure scenarios include:

- Component communication failures
- Transaction pipeline errors  
- Storage persistence problems
- Network connectivity issues

These conditions suggest that despite successful component startup, the integrated system cannot function correctly and requires a fresh recovery attempt.

## Redundancy Benefits

This dual storage strategy ensures that configuration information is available regardless of how future processes need to access it. Whether a coordinator needs the complete system state or a component needs specific configuration details, the appropriate storage format is available.

## Input Parameters

- Complete system configuration from Transaction System Layout
- Operational transaction processing components validated in previous phase

## Output Results

- Durably persisted configuration validated through successful system transaction
- System ready for operational monitoring setup

## Rationale

System transaction validates transaction pipeline while durably storing configuration; failure indicates fundamental problems requiring restart rather than continued recovery attempts.

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/persistence_phase.ex`

## Next Phase

Proceeds to [Monitoring](14-monitoring.md) to establish operational monitoring and complete recovery.