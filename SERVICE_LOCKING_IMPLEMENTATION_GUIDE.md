# Service Locking Implementation Guide

## Core Problem

The Lock Services Phase currently locks **ALL available services** indiscriminately via `lock_available_services(context.available_services, ...)`. This causes:

- Resource waste and state confusion
- Services in wrong states for their intended purpose
- Recovery failures when services aren't properly locked for recruitment

**Root Issue**: Should only lock services we actually need, when we need them.

## Required Changes

### 1. Modify Lock Services Phase

**File**: `/lib/bedrock/control_plane/director/recovery/lock_services_phase.ex:32`

**Current Code**:
```elixir
lock_available_services(context.available_services, recovery_attempt.epoch, 200, context)
```

**New Code**:
```elixir
services_to_lock = extract_old_system_services(context.old_transaction_system_layout, context.available_services)
lock_available_services(services_to_lock, recovery_attempt.epoch, 200, context)
```

**Add Function**:
```elixir
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

### 2. Verify Recruitment Phases

**Files to Check**:
- `/lib/bedrock/control_plane/director/recovery/recruit_logs_to_fill_vacancies.ex`
- `/lib/bedrock/control_plane/director/recovery/recruit_storage_to_fill_vacancies.ex`

**Requirement**: Ensure newly recruited services are locked and added to `transaction_services` during recruitment.

### 3. Update Service Requirements

Services **must** be locked before `recover_from` can be called (per `/bedrock/data_plane/log/shale/recovery.ex:20-21`).

## Expected Behavior

### Epoch 1 (First-time initialization)
- `services_to_lock = %{}` (no old system exists)
- Services locked **only during recruitment phases** as they're assigned
- Final layout contains only recruited services

### Epoch 2+ (Recovery scenarios)
- `services_to_lock = %{"old_service_id" => %{...}}` (only services from old system layout)
- Old services locked in Phase 1 for data copying
- New services locked during recruitment for new roles
- Log replay can access both old (source) and new (target) services

## Implementation Strategy

1. **Phase 1**: Lock only services from old transaction system layout
2. **Recruitment**: Lock services as they're recruited and assigned
3. **Log Replay**: Use services that are guaranteed to be locked (old + recruited)
4. **Persistence**: Filter final layout to only referenced services (existing logic)

## Success Criteria

- Epoch 1: No services locked initially, services locked during recruitment
- Epoch 2+: Only old system services locked initially, new services locked during recruitment
- Log replay succeeds with selective locking
- Final layout only contains services referenced in logs/storage_teams
- No unnecessary service locking