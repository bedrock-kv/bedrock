# Bedrock Service Locking Analysis and Implementation Plan

## Problem Analysis

### Current Issue
The Lock Services Phase is locking **all available services** indiscriminately, regardless of whether they're needed for recovery or not. This causes:

1. **Resource waste**: Locking services that won't be used
2. **State confusion**: Services in wrong states for their intended purpose  
3. **Recovery failures**: Services expected to be available for recruitment aren't properly locked

### Evidence from Logs

**Epoch 1 (First-time initialization):**
- Available services: `["bwecaxvz", "gb6cddk5", "kilvu2af", "lmlpc4er", "lyf2nhwq", "zwtq7mfs"]`
- **ALL** services get locked and have PIDs in `transaction_services`
- But only `"bwecaxvz"` (log) and `"gb6cddk5"` (storage) are actually used in final layout
- **Should lock**: None initially (no old system to copy from)

**Epoch 2 (Recovery from previous system):**
- Old logs requiring recovery: `["bwecaxvz"]`  
- Available services: `["bwecaxvz", "gb6cddk5", "kilvu2af", ...]`
- System tries to recruit `"kilvu2af"` but it fails validation (no PID)
- **Should lock**: Only `["bwecaxvz"]` (service from old system we need to copy from)

### Service State Requirements (from code analysis)

**Log Recovery Requirements:**
```elixir
# From /bedrock/data_plane/log/shale/recovery.ex:20-21
def recover_from(t, _, _, _) when t.mode != :locked,
  do: {:error, :lock_required}
```

**Locking Process:**
```elixir  
# From /bedrock/data_plane/log/shale/locking.ex:16
def lock_for_recovery(t, epoch, director) do
  {:ok, %{t | mode: :locked, epoch: epoch, director: director}}
end
```

**Conclusion**: Services **must** be locked before `recover_from` can be called.

## Correct Service Locking Strategy

### Principle
**Lock only services we actually need, when we need them:**

1. **Services from old transaction system layout** - for data copying
2. **Services we create/recruit** - added during recruitment phases  
3. **Never** lock services we don't intend to use

### Phase-by-Phase Breakdown

#### Phase 1: Lock Services Phase
```elixir
# Current (WRONG):
services_to_lock = context.available_services  # ALL services

# Correct:
services_to_lock = extract_services_from_old_layout(context.old_transaction_system_layout)
# For epoch 1: services_to_lock = %{} (no old system)
# For epoch 2: services_to_lock = %{"bwecaxvz" => %{...}} (only old system services)
```

#### Phase 2: Recruitment Phases  
```elixir
# When recruiting new services, lock them as they're assigned:
case assign_service_to_role(service_id, role) do
  {:ok, assigned} -> 
    lock_service_for_recovery(service, epoch)
  # Add to transaction_services with PID
end
```

#### Phase 3: Log Replay Phase
```elixir
# Now all services needed for replay are guaranteed to be locked:
# - Old services: locked in Phase 1
# - New services: locked during recruitment
Log.recover_from(new_log_name_node, old_log_name_node, first_version, last_version)
```

## Implementation Plan

### Step 1: Modify Lock Services Phase
**File**: `/lib/bedrock/control_plane/director/recovery/lock_services_phase.ex`

**Changes**:
```elixir
def execute(%{state: :lock_available_services} = recovery_attempt, context) do
  # OLD: lock_available_services(context.available_services, ...)
  # NEW: 
  services_to_lock = extract_old_system_services(context.old_transaction_system_layout, context.available_services)
  lock_available_services(services_to_lock, recovery_attempt.epoch, 200, context)
end

defp extract_old_system_services(old_layout, available_services) do
  old_service_ids = 
    (Map.keys(Map.get(old_layout, :logs, %{})) ++
     Enum.flat_map(Map.get(old_layout, :storage_teams, []), & &1.storage_ids))
    |> MapSet.new()
  
  available_services
  |> Enum.filter(fn {service_id, _} -> 
    MapSet.member?(old_service_ids, service_id)
  end)
  |> Map.new()
end
```

### Step 2: Modify Recruitment Phases
**Files**: 
- `/lib/bedrock/control_plane/director/recovery/recruit_logs_to_fill_vacancies.ex`
- `/lib/bedrock/control_plane/director/recovery/recruit_storage_to_fill_vacancies.ex`

**Changes**: Ensure newly recruited services get locked and added to `transaction_services`

**Current issue**: `extract_existing_log_services` tries to find services in `available_services` but they might not be locked. Need to lock them during recruitment.

### Step 3: Verify Log Replay Phase  
**File**: `/lib/bedrock/control_plane/director/recovery/log_replay_phase.ex`

**Verification**: Ensure `get_log_name_node` can find services in `transaction_services` for both:
- Old services (locked in Phase 1)
- New services (locked during recruitment)

### Step 4: Update Service Filtering
**File**: `/lib/bedrock/control_plane/director/recovery/persistence_phase.ex`

**Current**: `filter_services_for_layout` is correct - only include services referenced in layout
**Verification**: Ensure this works with selective locking

## Internal Consistency Checks

### Check 1: First-Time Initialization (Epoch 1)
```
old_transaction_system_layout.logs = %{} (empty)
services_to_lock = %{} (empty - correct!)
recruitment_phase: locks services as they're recruited
final_layout: only contains recruited services (correct!)
```

### Check 2: Recovery (Epoch 2+)  
```
old_transaction_system_layout.logs = %{"bwecaxvz" => [...]}
services_to_lock = %{"bwecaxvz" => %{...}} (only old service - correct!)
recruitment_phase: locks "kilvu2af" when recruited
log_replay: can access both "bwecaxvz" (old) and "kilvu2af" (new) (correct!)
```

### Check 3: Service State Transitions
```
Available Service → Lock (mode: :locked) → Recovery/Use → Final Layout
Old services: bwecaxvz → locked in Phase 1 → used for copy source → not in new layout
New services: kilvu2af → locked during recruitment → used as copy target → in new layout
```

### Check 4: Validation Phase Compatibility
```
validation_phase: checks transaction_services for PID info
transaction_services: contains only locked services (old + recruited)
layout.services: filtered subset of transaction_services (only referenced services)
```

## Potential Issues and Mitigations

### Issue 1: Race Conditions
**Problem**: Service might become unavailable between discovery and locking
**Mitigation**: Existing error handling in `lock_available_services` already handles this

### Issue 2: Epoch Conflicts  
**Problem**: Service locked by newer director
**Mitigation**: Existing `newer_epoch_exists` handling already addresses this

### Issue 3: Missing Services During Recruitment
**Problem**: Recruited service not available for locking  
**Mitigation**: Recruitment phase should handle service unavailability gracefully

## Testing Strategy

### Unit Tests
1. `extract_old_system_services` with various old layouts
2. Selective locking with partial service availability
3. Service state transitions (unlocked → locked → used)

### Integration Tests  
1. Epoch 1 scenario: no services locked initially, services locked during recruitment
2. Epoch 2 scenario: only old services locked, new services locked during recruitment
3. Multi-epoch scenario: verify service cleanup between epochs

### End-to-End Tests
1. c1/c2 scenario with component failure and recovery
2. Verify log replay works with selective locking
3. Verify final layout only contains referenced services

## Code Files to Modify

1. **Primary**: `/lib/bedrock/control_plane/director/recovery/lock_services_phase.ex`
2. **Secondary**: `/lib/bedrock/control_plane/director/recovery/recruit_logs_to_fill_vacancies.ex` 
3. **Verification**: `/lib/bedrock/control_plane/director/recovery/log_replay_phase.ex`
4. **Tests**: Add new test cases for selective locking behavior

## Success Criteria

1. **Epoch 1**: `transaction_services` only contains services actually used in layout
2. **Epoch 2**: Only old system services locked initially, new services locked during recruitment  
3. **Log replay**: Successfully accesses both old and new services via `{name, node}` references
4. **Final layout**: Only contains services referenced in logs/storage_teams
5. **Resource efficiency**: No unnecessary service locking

## Risk Assessment

**Low Risk**: Changes are isolated to specific phases with clear interfaces
**Medium Risk**: Need to ensure recruitment phases properly lock services  
**High Risk**: None identified - existing error handling should cover edge cases

The plan maintains backward compatibility while fixing the core issue of over-aggressive service locking.