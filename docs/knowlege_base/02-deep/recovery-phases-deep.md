# Recovery Phases Deep Reference

**Complete implementation guide covering every phase of Bedrock's recovery process.**

This document provides comprehensive coverage of all recovery phases, their implementations, state transitions, and code references. Use this as the definitive technical reference for understanding, debugging, or extending recovery functionality.

## Recovery Architecture Overview

Recovery is implemented as a linear state machine with 15 distinct phases. Each phase either:
- **Transitions** to the next phase with updated state
- **Stalls** with a reason, waiting for conditions to change
- **Completes** recovery successfully

### Phase Flow Control

Recovery flows through phases via the `run_recovery_attempt/3` function in `recovery.ex:171`:

```elixir
case next_phase_module.execute(t, context) do
  {completed_attempt, :completed} -> {:ok, completed_attempt}
  {stalled_attempt, {:stalled, _reason} = stalled} -> {stalled, stalled_attempt}
  {updated_attempt, next_next_phase_module} -> 
    updated_attempt |> run_recovery_attempt(context, next_next_phase_module)
end
```

### Phase Implementation Pattern

All phases use the `RecoveryPhase` behavior with a single `execute/2` function:

```elixir
@callback execute(RecoveryAttempt.t(), context()) ::
  {RecoveryAttempt.t(), :completed}
  | {RecoveryAttempt.t(), {:stalled, reason()}}
  | {RecoveryAttempt.t(), next_phase_module :: module()}
```

## Phase-by-Phase Implementation

### Phase 1: StartupPhase
**File**: `startup_phase.ex`  
**Purpose**: Recovery entry point and timing baseline

```elixir
def execute(%RecoveryAttempt{} = recovery_attempt, _context) do
  updated_attempt = %{recovery_attempt | started_at: now()}
  {updated_attempt, Bedrock.ControlPlane.Director.Recovery.LockingPhase}
end
```

**Key Responsibilities**:
- Records exact recovery start timestamp (`recovery.ex:44`)
- Provides timing baseline for recovery duration metrics
- Always succeeds, never stalls

**Next Phase**: LockingPhase

---

### Phase 2: LockingPhase
**File**: `locking_phase.ex`  
**Purpose**: Establish exclusive director control over old system services

**Core Implementation**:
```elixir
old_system_services = extract_old_system_services(
  context.old_transaction_system_layout,
  context.available_services
)

lock_old_system_services(old_system_services, recovery_attempt.epoch, context)
```

**Lock Process** (`locking_phase.ex:82`):
1. Extract services from old transaction system layout
2. Attempt to lock each service in parallel using `Task.async_stream`
3. Services accept locks only from newer epochs
4. Older epoch services terminate when they detect newer epochs

**Critical Branch Point** (`locking_phase.ex:42`):
- **No services locked**: → InitializationPhase (new cluster)
- **Services locked**: → LogRecoveryPlanningPhase (existing cluster recovery)

**Stall Conditions**:
- `:newer_epoch_exists`: Another director has a higher epoch

**Next Phase**: InitializationPhase OR LogRecoveryPlanningPhase

---

### Phase 3A: InitializationPhase (New Cluster Path)
**File**: `initialization_phase.ex`  
**Purpose**: Create initial transaction system layout for new clusters

**Implementation** (`initialization_phase.ex:29`):
```elixir
log_vacancies = 1..context.cluster_config.parameters.desired_logs 
  |> Enum.map(&{:vacancy, &1})

storage_team_vacancies = 1..context.cluster_config.parameters.desired_replication_factor 
  |> Enum.map(&{:vacancy, &1})
```

**Key Operations**:
- Creates log vacancies based on `desired_logs` parameter
- Creates storage team vacancies for replication factor
- Divides keyspace evenly: `key_range(<<>>, <<0xFF>>)` and `key_range(<<0xFF>>, :end)`
- Sets initial version state: `durable_version: 0`, `version_vector: {0, 0}`

**Always Succeeds**: Only creates in-memory structures

**Next Phase**: LogRecruitmentPhase

---

### Phase 3B: LogRecoveryPlanningPhase (Existing Cluster Path)
**File**: `log_recovery_planning_phase.ex`  
**Purpose**: Identify logs containing data requiring preservation

**Core Algorithm** (`log_recovery_planning_phase.ex:29`):
```elixir
determine_old_logs_to_copy(
  context.old_transaction_system_layout |> Map.get(:logs, %{}),
  recovery_attempt.log_recovery_info_by_id,
  context.cluster_config.parameters.desired_logs |> determine_quorum()
)
```

**Version Vector Calculation** (`log_recovery_planning_phase.ex:109`):
1. Extract version ranges from each log's recovery info
2. Find combinations that meet quorum requirements
3. Calculate consensus version vector: `{max(oldest), min(newest)}`
4. Rank by range size, preferring larger ranges

**Quorum Logic** (`log_recovery_planning_phase.ex:135`):
```elixir
def determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)
```

**Stall Conditions**:
- `:unable_to_meet_log_quorum`: Insufficient logs available

**Next Phase**: VacancyCreationPhase

---

### Phase 4: VacancyCreationPhase
**File**: `vacancy_creation_phase.ex`  
**Purpose**: Create placeholders for missing services

**Log Vacancy Creation** (`vacancy_creation_phase.ex:55`):
```elixir
def create_vacancies_for_logs(logs, desired_logs) do
  {updated_logs, _} = logs
    |> Enum.group_by(&Enum.sort(elem(&1, 1)), &elem(&1, 0))
    |> Enum.reduce({%{}, 1}, fn {tags, _ids}, {acc_map, vacancy_counter} ->
      new_vacancies = vacancy_counter..(vacancy_counter + desired_logs - 1)
        |> Enum.map(&{{:vacancy, &1}, tags})
        |> Map.new()
      {Map.merge(acc_map, new_vacancies), vacancy_counter + desired_logs}
    end)
end
```

**Storage Team Vacancy Logic** (`vacancy_creation_phase.ex:106`):
1. Group storage teams by tag sets
2. Calculate needed vacancies: `desired_replication - current_count`
3. Create numbered vacancy placeholders: `{:vacancy, 0}`, `{:vacancy, 1}`, etc.

**Always Succeeds**: Only modifies in-memory structures

**Next Phase**: VersionDeterminationPhase

---

### Phase 5: VersionDeterminationPhase
**File**: `version_determination_phase.ex`  
**Purpose**: Determine highest durable version and identify degraded teams

**Durable Version Calculation** (`version_determination_phase.ex:60`):
```elixir
def determine_durable_version(teams, info_by_id, quorum) do
  Enum.zip(teams |> Enum.map(& &1.tag),
           teams |> Enum.map(&determine_durable_version_and_status_for_storage_team(&1, info_by_id, quorum)))
  |> Enum.reduce({nil, [], [], []}, fn
    {tag, {:ok, version, :healthy}}, {min_version, healthy, degraded, failed} ->
      {smallest_version(version, min_version), [tag | healthy], degraded, failed}
    # ... pattern matching for degraded and failed cases
  end)
end
```

**Team Health Assessment** (`version_determination_phase.ex:99`):
- **Healthy**: Team has exactly `quorum` replicas at durable version
- **Degraded**: Team has `quorum` replicas but missing some
- **Failed**: Team has insufficient replicas (< quorum)

**Stall Conditions**:
- `{:insufficient_replication, failed_tags}`: Some teams cannot meet quorum

**Next Phase**: LogRecruitmentPhase

---

### Phase 6: LogRecruitmentPhase
**File**: `log_recruitment_phase.ex`  
**Purpose**: Fill log vacancies with available or new services

**Recruitment Strategy** (`log_recruitment_phase.ex:92`):
```elixir
def fill_log_vacancies(logs, old_system_log_ids, available_log_ids, available_nodes) do
  vacancies = all_vacancies(logs)
  candidates_ids = available_log_ids |> MapSet.difference(old_system_log_ids)
  
  if n_vacancies <= n_candidates do
    {:ok, replace_vacancies_with_log_ids(logs, assignments), []}
  else
    needed_workers = n_vacancies - n_candidates
    # Create new worker IDs and assign to nodes round-robin
  end
end
```

**Worker Creation Process** (`log_recruitment_phase.ex:154`):
1. Calculate needed workers beyond available services
2. Assign workers to nodes using round-robin distribution
3. Create workers via `Foreman.new_worker/4` calls
4. Lock all recruited services (existing and new)

**Stall Conditions**:
- `{:insufficient_nodes, needed, available}`: Not enough nodes for workers
- `{:failed_to_lock_recruited_service, log_id, reason}`: Locking failures

**Next Phase**: StorageRecruitmentPhase

---

### Phase 7: StorageRecruitmentPhase
**File**: `storage_recruitment_phase.ex`  
**Purpose**: Fill storage team vacancies, preferring existing services

**Vacancy Filling Logic** (`storage_recruitment_phase.ex:159`):
```elixir
def fill_storage_team_vacancies(storage_teams, assigned_storage_ids, available_storage_ids, available_nodes) do
  vacancies = assigned_storage_ids |> MapSet.filter(&vacancy?/1)
  
  if n_vacancies <= n_candidates do
    {:ok, replace_vacancies_with_storage_ids(storage_teams, assignments), []}
  else
    # Create new workers as needed
  end
end
```

**Key Differences from Log Recruitment**:
- Preserves existing storage services (contain persistent data)
- Only creates new workers when absolutely necessary
- Excludes old system storage services from recruitment pool

**Stall Conditions**:
- `{:insufficient_nodes, needed, available}`: Not enough nodes
- Worker creation or locking failures

**Next Phase**: LogReplayPhase

---

### Phase 8: LogReplayPhase
**File**: `log_replay_phase.ex`  
**Purpose**: Copy committed transactions from old logs to new logs

**Replay Implementation** (`log_replay_phase.ex:57`):
```elixir
def replay_old_logs_into_new_logs(old_log_ids, new_log_ids, {first_version, last_version}, recovery_attempt, context) do
  new_log_ids
  |> pair_with_old_log_ids(old_log_ids)
  |> Task.async_stream(fn {new_log_id, old_log_id} ->
    Log.recover_from(new_log_name_node, old_log_name_node, first_version, last_version)
  end, ordered: false)
  |> process_replay_results()
end
```

**Log Pairing Strategy** (`log_replay_phase.ex:172`):
- If no old logs: pairs new logs with `:none` (empty recovery)
- If old logs exist: cycles old logs across new logs for distribution

**Name Resolution** (`log_replay_phase.ex:149`):
Resolves log IDs to `{name, node}` tuples for GenServer calls, checking:
1. `transaction_services` for locked services with PIDs
2. `available_services` for newly created services

**Stall Conditions**:
- `:newer_epoch_exists`: Detected during replay operations
- `{:failed_to_copy_some_logs, failures}`: Replay failures

**Next Phase**: DataDistributionPhase

---

### Phase 9: DataDistributionPhase
**File**: `data_distribution_phase.ex`  
**Purpose**: Create resolver descriptors from storage teams

**Resolver Descriptor Creation** (`data_distribution_phase.ex:40`):
```elixir
defp create_resolver_descriptors_from_storage_teams(storage_teams) do
  storage_teams
  |> Enum.map(fn storage_team ->
    {start_key, _end_key} = storage_team.key_range
    resolver_descriptor(start_key, nil)
  end)
  |> Enum.uniq_by(& &1.start_key)
  |> Enum.sort_by(& &1.start_key)
end
```

**Note**: Data repair functionality is simplified in current implementation. 
The phase focuses on resolver descriptor generation rather than active data repair.

**Always Succeeds**: Only creates resolver descriptors

**Next Phase**: SequencerStartupPhase

---

### Phase 10: SequencerStartupPhase
**File**: `sequencer_startup_phase.ex`  
**Purpose**: Start the global version sequencer

**Sequencer Configuration** (`sequencer_startup_phase.ex:52`):
```elixir
defp build_sequencer_child_spec(recovery_attempt) do
  {_first_version, last_committed_version} = recovery_attempt.version_vector
  
  Sequencer.child_spec(
    director: self(),
    epoch: recovery_attempt.epoch,
    last_committed_version: last_committed_version,
    otp_name: recovery_attempt.cluster.otp_name(:sequencer)
  )
end
```

**Startup Process**:
1. Build child spec with current epoch and last committed version
2. Start supervised process on current node
3. Register sequencer PID in recovery state

**Stall Conditions**:
- `{:failed_to_start, :sequencer, node, reason}`: Startup failures

**Next Phase**: ProxyStartupPhase

---

### Phase 11: ProxyStartupPhase
**File**: `proxy_startup_phase.ex`  
**Purpose**: Start commit proxy components for horizontal transaction processing scalability

**Node Selection Logic**:
```elixir
available_commit_proxy_nodes = Map.get(context.node_capabilities, :coordination, [])
```

**Proxy Startup Logic** (`proxy_startup_phase.ex:72`):
```elixir
def define_commit_proxies(n_proxies, cluster, epoch, director, available_nodes, start_supervised, lock_token) do
  if Enum.empty?(available_nodes) do
    {:error, {:insufficient_nodes, :no_coordination_capable_nodes, n_proxies, 0}}
  else
    child_spec = child_spec_for_commit_proxy(cluster, epoch, director, lock_token)
    
    # Round-robin distribute proxies across available nodes
    available_nodes
    |> distribute_proxies_round_robin(n_proxies)
    |> Task.async_stream(fn node ->
      start_supervised.(child_spec, node)
    end, ordered: false)
    |> collect_proxy_pids()
  end
end
```

**Distribution Strategy**:
- Uses coordination-capable nodes from `context.node_capabilities.coordination`
- Round-robin distribution across available nodes for fault tolerance
- Starts up to `desired_commit_proxies` using `Stream.cycle()` for even distribution
- Uses parallel startup with `Task.async_stream`
- Validates sufficient nodes are available before startup

**Error Conditions**:
- `{:insufficient_nodes, :no_coordination_capable_nodes, requested, available}`: No coordination nodes available
- `{:failed_to_start, :commit_proxy, node, reason}`: Individual proxy startup failures

**Next Phase**: ResolverStartupPhase

---

### Phase 12: ResolverStartupPhase
**File**: `resolver_startup_phase.ex`  
**Purpose**: Start MVCC conflict detection resolvers

**Resolver Range Generation** (`resolver_startup_phase.ex:101`):
```elixir
defp generate_resolver_ranges(resolvers) do
  resolvers
  |> Enum.map(& &1.start_key)
  |> Enum.sort()
  |> Enum.concat([:end])
  |> Enum.chunk_every(2, 1, :discard)  # Creates overlapping pairs
end
```

**Storage Team Mapping** (`resolver_startup_phase.ex:136`):
Uses ETS match specs to find storage teams covering each resolver range:
```elixir
def storage_team_tags_covering_range(storage_teams, min_key, max_key_exclusive) do
  :ets.match_spec_run(storage_teams, :ets.match_spec_compile([
    {{{:"$1", :"$2"}, :"$3", :_},
     [{:or, {:<, min_key, :"$2"}, {:==, :end, :"$2"}},
      {:and, {:or, {:<, :"$1", max_key_exclusive}, {:==, :end, max_key_exclusive}}}],
     [:"$3"]}
  ]))
end
```

**Log Assignment Process** (`resolver_startup_phase.ex:158`):
1. Generate key ranges from resolver descriptors
2. Map storage teams to each range
3. Assign matching logs to each resolver
4. Start resolvers with recovery data via `Resolver.recover_from/5`

**Stall Conditions**:
- `{:failed_to_start, :resolver, node, reason}`: Resolver startup failures

**Next Phase**: ValidationPhase

---

### Phase 13: ValidationPhase
**File**: `validation_phase.ex`  
**Purpose**: Final readiness checks before system construction

**Validation Checks** (`validation_phase.ex:43`):
```elixir
defp validate_recovery_state(recovery_attempt) do
  with :ok <- validate_sequencer(recovery_attempt.sequencer),
       :ok <- validate_commit_proxies(recovery_attempt.proxies),
       :ok <- validate_resolvers(recovery_attempt.resolvers),
       :ok <- validate_logs(recovery_attempt.logs, recovery_attempt.transaction_services) do
    :ok
  end
end
```

**Log Service Validation** (`validation_phase.ex:96`):
Ensures all log IDs have corresponding operational services:
```elixir
missing_services = log_ids |> Enum.reject(fn log_id ->
  case Map.get(transaction_services, log_id) do
    %{status: {:up, pid}} when is_pid(pid) -> true
    _ -> false
  end
end)
```

**Validation Rules**:
- Sequencer: Must be a valid PID
- Commit Proxies: Non-empty list of PIDs
- Resolvers: Non-empty list of `{start_key, pid}` tuples
- Logs: All log IDs must have `{:up, pid}` status in transaction services

**Stall Conditions**:
- `{:recovery_system_failed, {:invalid_recovery_state, reason}}`: Any validation failure

**Next Phase**: TransactionSystemLayoutPhase

---

### Phase 14: TransactionSystemLayoutPhase
**File**: `transaction_system_layout_phase.ex`  
**Purpose**: Build transaction system layout and unlock services

**Layout Construction** (`transaction_system_layout_phase.ex:57`):
```elixir
defp build_transaction_system_layout(recovery_attempt, context) do
  {:ok, %{
    id: TransactionSystemLayout.random_id(),
    epoch: recovery_attempt.epoch,
    director: self(),
    sequencer: recovery_attempt.sequencer,
    rate_keeper: nil,
    proxies: recovery_attempt.proxies,
    resolvers: recovery_attempt.resolvers,
    logs: recovery_attempt.logs,
    storage_teams: recovery_attempt.storage_teams,
    services: build_services_for_layout(recovery_attempt, context)
  }}
end
```

**Service Unlocking Process** (`transaction_system_layout_phase.ex:151`):
1. **Commit Proxies**: Unlock via `CommitProxy.recover_from/3` with layout
2. **Storage Servers**: Unlock via `Storage.unlock_after_recovery/3` with durable version

**Unlocking Rationale**:
Services were locked during recruitment to prevent interference. Now they must be unlocked to participate in the system transaction during persistence.

**Stall Conditions**:
- `{:recovery_system_failed, {:unlock_failed, reason}}`: Service unlocking failures

**Next Phase**: PersistencePhase

---

### Phase 15: PersistencePhase
**File**: `persistence_phase.ex`  
**Purpose**: Persist configuration via system transaction

**System Transaction Construction** (`persistence_phase.ex:60`):
```elixir
defp build_system_transaction(epoch, cluster_config, transaction_system_layout, cluster) do
  encoded_config = Persistence.encode_for_storage(cluster_config, cluster)
  encoded_layout = Persistence.encode_transaction_system_layout_for_storage(transaction_system_layout, cluster)
  
  build_monolithic_keys(epoch, encoded_config, encoded_layout)
  |> Map.merge(build_decomposed_keys(epoch, cluster_config, transaction_system_layout, cluster))
  |> then(&{nil, &1})
end
```

**Dual Storage Strategy**:
- **Monolithic Keys**: Support coordinator handoff scenarios
  - `SystemKeys.config_monolithic()`: Complete cluster config
  - `SystemKeys.layout_monolithic()`: Complete layout
- **Decomposed Keys**: Enable targeted component access
  - `SystemKeys.cluster_*()`: Individual cluster parameters
  - `SystemKeys.layout_*()`: Individual layout components

**Transaction Submission** (`persistence_phase.ex:192`):
```elixir
defp submit_system_transaction(system_transaction, proxies, context) do
  commit_fn = Map.get(context, :commit_transaction_fn, &CommitProxy.commit/2)
  
  proxies |> Enum.random() |> then(&commit_fn.(&1, system_transaction))
end
```

**Critical Behavior**: 
System transaction failure causes director exit rather than retry. This indicates fundamental problems requiring coordinator restart with new epoch.

**Stall Conditions**:
- `{:recovery_system_failed, reason}`: System transaction failures

**Next Phase**: MonitoringPhase

---

### Phase 16: MonitoringPhase
**File**: `monitoring_phase.ex`  
**Purpose**: Establish component monitoring and complete recovery

**Process Monitoring Setup** (`monitoring_phase.ex:37`):
```elixir
defp extract_pids_to_monitor(layout) do
  resolver_pids = layout.resolvers |> Enum.map(fn {_start_key, pid} -> pid end)
  service_pids = layout.services
    |> Enum.filter(fn {_service_id, service} -> service.kind != :storage end)
    |> Enum.map(fn {_service_id, %{status: {:up, pid}}} -> pid end)
  
  Enum.concat([[layout.sequencer], layout.proxies, resolver_pids, service_pids])
end
```

**Monitoring Philosophy**:
Implements fail-fast strategy using `Process.monitor/1`. Any monitored component failure causes director shutdown, triggering coordinator to restart recovery with new epoch.

**Excluded from Monitoring**:
- **Storage Servers**: Handle failures independently through data distribution
- **Gateways**: Client-facing components with local failure handling

**Return Value**: `{recovery_attempt, :completed}`

This signals recovery completion to the main recovery orchestrator. MonitoringPhase is the final phase of recovery.

---

## Recovery State Management

### RecoveryAttempt Structure
**File**: `config/recovery_attempt.ex`

Key fields maintained throughout recovery:
- `cluster`: Cluster configuration
- `epoch`: Current recovery epoch
- `attempt`: Retry counter for this epoch
- `started_at`: Recovery start timestamp
- `durable_version`: Highest committed version across storage
- `version_vector`: `{oldest_version, newest_version}` tuple
- `logs`: Log ID → descriptor mapping
- `storage_teams`: Storage team descriptors
- `transaction_services`: Service ID → service info mapping
- `service_pids`: Service ID → PID mapping for monitoring

### Context Structure
**File**: `recovery.ex:40`

Recovery context passed to all phases:
- `cluster_config`: Complete cluster configuration
- `old_transaction_system_layout`: Previous layout for data preservation
- `node_capabilities`: Available nodes by capability type
- `lock_token`: Unique token for service locking
- `available_services`: Service ID → location mapping
- `coordinator`: Coordinator PID for config updates

## Recovery Failure Modes

### Stall Conditions
Recovery stalls (rather than failing) in these scenarios:
- **Resource Constraints**: Insufficient nodes, services, or storage capacity
- **Quorum Failures**: Unable to meet log or storage quorum requirements
- **Service Unavailability**: Required services unreachable or unresponsive
- **Lock Conflicts**: Another director has newer epoch

### Immediate Failures (Director Exit)
Recovery fails immediately for:
- **System Transaction Failures**: Cannot persist configuration
- **Epoch Conflicts**: Newer epoch detected during operations
- **Critical Component Failures**: Monitored process deaths

### Recovery Retry Logic
**File**: `recovery.ex:131`

Stalled recovery persists state and waits for environmental changes:
```elixir
{{:stalled, reason}, stalled} ->
  trace_recovery_stalled(Interval.between(stalled.started_at, now()), reason)
  
  t |> Map.update!(:config, fn config ->
    config |> Map.put(:recovery_attempt, stalled)
  end) |> persist_config()
```

Recovery retries when:
- New nodes join the cluster
- Failed services become available
- Network partitions heal
- Coordinator receives director failure notification

## Integration Points

### Coordinator Integration
**File**: `coordinator.ex`

The Coordinator:
- Starts Directors with populated service directory
- Receives recovery completion/failure notifications
- Persists recovery state through Raft consensus
- Triggers retry when environmental conditions change

### Service Integration
**File**: Various service modules (`log.ex`, `storage.ex`, etc.)

Services support recovery through:
- `lock_for_recovery/2`: Exclusive control establishment
- `recover_from/4`: Data migration and initialization
- `unlock_after_recovery/3`: Post-recovery preparation

### Telemetry Integration
**File**: `recovery/telemetry.ex`

Recovery emits detailed telemetry for:
- Phase transitions and timing
- Resource availability and allocation
- Error conditions and stall reasons
- Component startup and validation

## Testing and Debugging

### Recovery Test Support
**File**: `test/support/recovery_test_support.ex`

Provides utilities for:
- Mocking service availability
- Simulating failure conditions
- Validating recovery state transitions
- Testing recovery phase isolation

### Common Debugging Commands

1. **Check Recovery State**:
   ```elixir
   :sys.get_state(coordinator_pid).config.recovery_attempt
   ```

2. **Monitor Recovery Progress**:
   ```elixir
   :telemetry.attach_many("recovery_debug", [
     [:bedrock, :recovery, :phase_transition],
     [:bedrock, :recovery, :stalled]
   ], &IO.inspect/4, nil)
   ```

3. **Examine Service Availability**:
   ```elixir
   :sys.get_state(director_pid).services
   ```

4. **Trace Recovery Execution**:
   Enable tracing in `recovery/tracing.ex` for detailed execution flow.

## See Also

- **[Recovery Guide](../01-guides/recovery-guide.md)**: High-level recovery concepts
- **[Architecture Deep](architecture-deep.md)**: System architecture context
- **[Components Quick](../00-quick/components-quick.md)**: Component overview
- **[Debug Quick](../00-quick/debug-quick.md)**: Debugging commands

---

*This document covers all 16 recovery phases as implemented in the codebase. For phase-specific details, refer to the individual module files in `lib/bedrock/control_plane/director/recovery/`.*