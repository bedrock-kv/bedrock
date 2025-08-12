# TSL Validation

**The first line of defense against data corruption during recovery, ensuring type safety before any recovery operations begin.**

Before attempting to recover a distributed system, the recovery orchestrator must verify that the planned transaction system layout conforms to expected data types and structural requirements. TSL validation serves as the critical safety gate that prevents recovery from proceeding with malformed or incompatible system configurations.

## How It Works

TSL validation performs comprehensive type checking and structural validation on all components that will participate in the recovered transaction system:

**Type Safety Validation**: The `TSLTypeValidator.assert_type_safety!/1` function validates that all component types match expected interfaces—ensuring logs are properly structured, storage teams contain valid identifiers, and transaction components have correct process references.

**Structural Integrity**: Validates that the transaction system layout contains all required fields with appropriate data types, preventing runtime errors during subsequent recovery phases.

**Early Error Detection**: Catches configuration problems, type mismatches, and structural inconsistencies before they can cause failures in later recovery phases or corrupt system state.

## What Gets Validated

The validation process examines critical system components:

- **Transaction Components**: Sequencer process ID, commit proxy references, resolver key ranges and process mappings
- **Data Services**: Log descriptors and identifiers, storage team configurations and member lists
- **System Layout**: Epoch consistency, service mappings, and component relationships

## Critical Functions

1. **Prevent Corruption**: Stops recovery immediately if any component fails type validation, protecting the system from proceeding with invalid configurations

2. **Early Failure Detection**: Identifies problems at the beginning of recovery rather than during critical reconstruction phases when failures are more costly

3. **Type System Enforcement**: Ensures all components conform to Bedrock's type system requirements, preventing runtime type errors during recovery

## Recovery Flow

TSL validation operates as Phase 0 of the recovery sequence:

- **Success**: Type validation passes → proceed to [Service Locking](service-locking.md)
- **Failure**: Validation fails → recovery stalls immediately with detailed error information

This ensures that no recovery operations begin until the system layout is proven safe and structurally sound.

## Error Handling

- **Type Mismatch**: Any component that fails type validation immediately stalls recovery with `{:recovery_system_failed, {:invalid_recovery_state, reason}}`
- **Structural Problems**: Missing required fields or invalid data structures trigger immediate validation failure
- **Fatal Failures**: All validation failures are considered fatal—recovery cannot proceed without a valid TSL

The validation phase acts as a strict gatekeeper, ensuring that only properly formed transaction system layouts can proceed through the recovery process.

---

**Next Phase**: [Service Locking](service-locking.md) — Establishing exclusive recovery authority over previous system components