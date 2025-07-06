# Configuration Refactoring Migration Plan

This document outlines the migration from monolithic system keys to a hybrid approach that supports both monolithic and decomposed keys for better scalability and targeted consumption.

## Overview

The refactoring introduces a hybrid key storage approach where:
- **Monolithic keys** are maintained for coordinator epoch handoff and backward compatibility
- **Decomposed keys** enable targeted consumption by specific components
- **Atomic updates** ensure consistency between both formats via single system transactions

## Key Benefits

1. **Atomic Consistency**: All keys updated in single transaction
2. **Flexible Consumption**: Components read only what they need
3. **Easy Handoff**: Coordinator gets complete state in one key
4. **Future Extensibility**: Foundation for reactive monitoring
5. **No Circular Dependencies**: Director gets coordinator nodes from startup config

## System Key Schema

### Centralized Key Management

All system keys are now managed through the `Bedrock.SystemKeys` module:

```elixir
# Cluster configuration
SystemKeys.cluster_coordinators()          # List of coordinator nodes
SystemKeys.cluster_epoch()                 # Current cluster epoch
SystemKeys.cluster_policies_volunteer_nodes() # Policy settings

# Transaction layout  
SystemKeys.layout_sequencer()              # Current sequencer reference
SystemKeys.layout_proxies()                # List of commit proxy references
SystemKeys.layout_resolvers()              # List of resolver assignments

# Dynamic keys with IDs
SystemKeys.layout_log("log_123")           # Specific log configuration
SystemKeys.layout_storage_team("team_456") # Specific storage team config

# Legacy compatibility
SystemKeys.config_monolithic()             # Complete config for handoff
```

### Key Categories

#### Cluster Configuration Keys
- `\xff/system/cluster/coordinators` - List of coordinator nodes
- `\xff/system/cluster/epoch` - Current cluster epoch
- `\xff/system/cluster/policies/volunteer_nodes` - Allow volunteer nodes policy
- `\xff/system/cluster/parameters/*` - Various cluster parameters

#### Transaction System Layout Keys  
- `\xff/system/layout/sequencer` - Current sequencer reference
- `\xff/system/layout/proxies` - List of commit proxy references
- `\xff/system/layout/resolvers` - List of resolver assignments
- `\xff/system/layout/logs/{log_id}` - Individual log configurations
- `\xff/system/layout/storage/{team_id}` - Storage team assignments

#### Legacy Compatibility Keys
- `\xff/system/config` - Monolithic config (for coordinator handoff)
- `\xff/system/transaction_system_layout` - Monolithic layout (deprecated)

## Implementation Progress

### ✅ Phase 1: Foundation & Documentation (COMPLETED)

#### Documentation
- ✅ **Created `Bedrock.SystemKeys` module** with centralized key definitions
- ✅ **Created migration plan document** (`docs/plans/configuration-refactoring-plan.md`)
- ⏳ **Document system key schema** in knowledge base (`.clinerules/`)
- ⏳ **Update component responsibility matrix** in implementation guides
- ⏳ **Document epoch-to-epoch handoff process** in recovery internals

#### Key Module Implementation
- ✅ **Define cluster configuration keys** (coordinators, epoch, policies, parameters)
- ✅ **Define transaction layout keys** (sequencer, proxies, resolvers, logs, storage)
- ✅ **Define legacy compatibility keys** (monolithic config, layout)
- ✅ **Add dynamic key builders** (for log IDs, storage team IDs)
- ✅ **Write comprehensive tests** for key module

### ✅ Phase 2: Fix Immediate Circular Dependencies (COMPLETED)

#### Director Changes
- ✅ **Remove Gateway dependency** from Director startup
- ✅ **Get coordinator nodes from startup config** instead of Gateway call
- ✅ **Update persistence phase** to use coordinator nodes from Director state
- ✅ **Test Director can start without Gateway** being operational

#### CommitProxy Changes  
- ✅ **Remove circular dependency** (already completed in previous session)
- ✅ **Extract layout from system transactions** (already implemented)
- ✅ **Verify CommitProxy works with nil layout initially**

#### Integration Testing
- ✅ **Test full recovery completes** without deadlocks (via existing test suite)
- ✅ **Verify system transaction succeeds** after recovery
- ✅ **Test multi-node cluster startup** works end-to-end

### ✅ Phase 3: Implement Hybrid Key Storage (COMPLETED)

#### Persistence Updates
- ✅ **Update persistence phase** to write decomposed keys using `SystemKeys`
- ✅ **Maintain monolithic config write** for coordinator handoff
- ✅ **Ensure atomic writes** of all keys in single transaction
- ✅ **Add comprehensive logging** of what keys are written

#### Backward Compatibility
- ✅ **Ensure all reads still use monolithic keys** during transition
- ✅ **Add fallback logic** for missing decomposed keys
- ✅ **Test upgrade scenarios** (old cluster → new code)

#### Testing
- ✅ **Test both key formats are written** atomically
- ✅ **Verify key consistency** between monolithic and decomposed
- ✅ **Test recovery with hybrid key storage**

### ⏳ Phase 4: Component Migration (FUTURE)

#### Component Updates
- ⏳ **Migrate Gateway** to read from decomposed keys
- ⏳ **Migrate other data plane components** as needed
- ⏳ **Remove fallback logic** once all components migrated
- ⏳ **Performance test** decomposed key reads vs monolithic

#### Cleanup
- ⏳ **Remove unused monolithic layout key** (keep config for handoff)
- ⏳ **Update all magic strings** to use `SystemKeys`
- ⏳ **Remove deprecated code paths**

## Component Information Requirements

### Coordinator
**Needs**: Complete cluster state for epoch handoff
**Reads**: `SystemKeys.config_monolithic()` (monolithic)
**Writes**: All system keys atomically

### Director  
**Needs**: Previous layout + cluster config for recovery planning
**Reads**: `SystemKeys.config_monolithic()` at startup
**Writes**: All system keys after successful recovery

### Gateway
**Needs**: Coordinator discovery + current transaction layout
**Reads**: `SystemKeys.cluster_coordinators()`, `SystemKeys.layout_*`
**Writes**: None

### CommitProxy
**Needs**: Current resolvers and logs for transaction routing
**Reads**: `SystemKeys.layout_resolvers()`, `SystemKeys.layout_logs_prefix()`
**Writes**: None

## Migration Strategy

### Current State (Phase 2 Complete)
- Director gets coordinator nodes from startup config (no Gateway dependency)
- SystemKeys module provides centralized key management
- All tests passing, circular dependency resolved

### Next Steps (Phase 3)
1. **Update persistence phase** to write both monolithic + decomposed keys
2. **Keep all reads using monolithic keys** for now
3. **Ensure atomic writes** of all keys in single transaction

### Future Phases
1. **Migrate component reads** to decomposed keys one by one
2. **Add reactive monitoring** foundation for key changes
3. **Remove deprecated monolithic keys** (except config for handoff)

## Benefits Achieved

### Immediate Benefits (Phase 2)
- ✅ **Circular dependency resolved**: Director no longer depends on Gateway
- ✅ **Centralized key management**: All system keys defined in one place
- ✅ **Type safety**: Compile-time errors for key typos
- ✅ **Clear separation**: System keys vs user keyspace

### Future Benefits (Phases 3-4)
- **Targeted updates**: Only update keys that actually changed
- **Selective monitoring**: Components watch only relevant keys
- **Atomic consistency**: All updates in single transaction
- **Reactive architecture**: Foundation for key change notifications

## Testing Strategy

### Completed Testing
- ✅ **SystemKeys module**: Comprehensive unit tests for all key functions
- ✅ **Coordinator bootstrap**: Tests for storage fallback logic
- ✅ **Director recovery**: Tests for coordinator node access
- ✅ **Integration tests**: Full test suite passing

### Future Testing
- **Hybrid key storage**: Test both formats written atomically
- **Component migration**: Test gradual migration to decomposed keys
- **Performance testing**: Compare monolithic vs decomposed key performance
- **Upgrade scenarios**: Test old cluster → new code transitions

## Error Handling

### Graceful Degradation
- **Storage unavailable**: Fall back to default configuration
- **Corrupted data**: BERT deserialization error recovery
- **Network issues**: Timeout handling for storage queries
- **Missing keys**: Fallback logic for missing decomposed keys

### Fail-Fast Behavior
- **System transaction failure**: Director exits, coordinator retries
- **Invalid configuration**: Clear error messages and fast failure
- **Component failures**: Immediate director restart

This refactoring provides a solid foundation for scalable configuration management while maintaining backward compatibility and system reliability.
