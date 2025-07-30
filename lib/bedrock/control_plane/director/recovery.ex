defmodule Bedrock.ControlPlane.Director.Recovery do
  @moduledoc """
  Bedrock's distributed system recovery orchestrator.

  This module implements the core recovery process that rebuilds the transaction system
  after failures, following a fast-recovery approach inspired by FoundationDB combined
  with Erlang/OTP's "let it crash" philosophy.

  ## Recovery Philosophy

  Bedrock combines two proven approaches to create a robust, fast-recovering system:

  ### "Let It Crash" (Erlang/OTP)
  - **Fast Failure Detection**: Use `Process.monitor/1` rather than complex health checking
  - **Immediate Failure Response**: Any critical component failure triggers immediate director shutdown
  - **Supervision Tree Restart**: Let Erlang's supervision trees handle automatic restart
  - **Fail-Fast Error Handling**: Prefer immediate failure over complex error recovery

  ### Fast Recovery Over Complex Error Handling (FoundationDB)
  - **Component Failure Triggers Full Recovery**: Any transaction system component failure causes complete recovery
  - **Process Suicide**: Processes terminate themselves when they detect newer generations
  - **Recovery Count Mechanism**: Each recovery increments an epoch counter for generation management
  - **Simple Failure Detection**: Use heartbeats and process monitoring, not complex availability checking

  ## When Recovery Triggers

  ### Critical Components (Recovery Triggers)
  Recovery is triggered when any of these components fail:
  - **Coordinator**: Raft consensus failure or network partition
  - **Director**: Recovery coordinator failure
  - **Sequencer**: Version assignment failure
  - **Commit Proxies**: Transaction batching failure
  - **Resolvers**: Conflict detection failure
  - **Transaction Logs**: Durability system failure

  ### Non-Critical Components (No Recovery)
  These failures do NOT trigger recovery:
  - **Storage Servers**: Data distributor handles storage failures
  - **Gateways**: Client interface failures are handled locally
  - **Rate Keeper**: Independent component with separate lifecycle

  ## Recovery State Machine

  The recovery process follows a linear state machine through these phases:

  1. `:start` → `StartPhase` - Initialize recovery attempt with timestamp
  2. `:lock_old_system_services` → `LockOldSystemServicesPhase` - Lock services from old system layout
  3. Branch point:
     - `:first_time_initialization` → `InitializationPhase` - Bootstrap new cluster
     - `:determine_old_logs_to_copy` → `LogDiscoveryPhase` - Find logs to recover
  4. `:determine_durable_version` → `DurableVersionPhase` - Find highest committed version
  5. `:create_vacancies` → `VacancyCreationPhase` - Create placeholders for missing services
  6. `:recruit_logs_to_fill_vacancies` → `RecruitLogsToFillVacanciesPhase` - Assign/create log workers
  7. `:recruit_storage_to_fill_vacancies` → `StorageRecruitmentPhase` - Assign/create storage workers
  8. `:replay_old_logs` → `LogReplayPhase` - Replay transactions from old logs
  9. `:repair_data_distribution` → `DataDistributionPhase` - Fix data distribution
  10. `:define_sequencer` → `SequencerPhase` - Start sequencer component
  11. `:define_commit_proxies` → `CommitProxyPhase` - Start commit proxies
  12. `:define_resolvers` → `ResolverPhase` - Start resolver components
  13. `:final_checks` → `ValidationPhase` - Final validation before persistence
  14. `:persist_system_state` → `PersistencePhase` - System transaction test and persist
  15. `:monitor_components` → `MonitoringPhase` - Set up component monitoring
  16. `:cleanup_obsolete_workers` → `WorkerCleanupPhase` - Clean up unused workers
  17. `:completed` - Recovery complete

  Any phase can transition to `{:stalled, reason}` which triggers retry logic.

  ## Epoch-Based Split-Brain Prevention

  Durable services use epoch management to prevent split-brain scenarios:
  - **Service Locking**: Director locks services with new epoch during recovery
  - **Old Epoch Services**: Services with older epochs stop participating
  - **New Epoch Services**: Only services locked with current epoch participate
  - **Fail-Safe**: Services refuse commands from directors with older epochs

  ## Error Handling Patterns

  - **Fail-Fast Implementation**: Exit immediately on component failure rather than complex recovery
  - **Epoch-Based Generation Management**: Each recovery increments epoch, components self-terminate on newer epochs
  - **Process Monitoring**: Use `Process.monitor/1` for component failure detection
  - **Immediate Director Exit**: Any transaction component failure triggers director termination

  ## Performance Characteristics

  - **Cold Start**: 5-15 seconds (depending on cluster size)
  - **Warm Restart**: 1-5 seconds (with persistent configuration)
  - **Component Failure**: Sub-second detection, 1-3 second restart
  - **Node Count**: Recovery time increases logarithmically with cluster size

  ## Implementation Guidelines

  1. **Always use `Process.monitor/1` for component monitoring**
  2. **Exit immediately on component failure - don't attempt recovery**
  3. **Use epoch counters for generation management**
  4. **Implement fail-fast error handling**
  5. **Test recovery paths extensively**
  6. **Accept recovery restart on node rejoin - don't prevent it**
  7. **Ensure durable services properly handle epoch transitions**

  ## See Also

  - `Bedrock.ControlPlane.Director.Recovery.RecoveryPhase` - Behavior for recovery phases
  - Individual phase modules in `Bedrock.ControlPlane.Director.Recovery.*Phase`
  - [Recovery Guide](https://github.com/your-org/bedrock/blob/main/lib/bedrock/control_plane/director/recovery.ex) - Comprehensive recovery documentation
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Director.NodeTracking
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.Internal.Time.Interval
  alias Bedrock.Service.Worker

  import Bedrock.Internal.Time, only: [now: 0]

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @type recovery_context :: %{
          cluster_config: Config.t(),
          old_transaction_system_layout: TransactionSystemLayout.t(),
          node_tracking: NodeTracking.t(),
          lock_token: binary(),
          available_services: %{Worker.id() => %{kind: atom(), last_seen: {atom(), node()}}},
          coordinator: pid()
        }

  @spec try_to_recover(State.t()) :: State.t()
  def try_to_recover(%{state: :starting} = t) do
    t
    |> setup_for_initial_recovery()
    |> do_recovery()
  end

  @spec try_to_recover(State.t()) :: State.t()
  def try_to_recover(%{state: :recovery} = t) do
    t
    |> setup_for_subsequent_recovery()
    |> do_recovery()
  end

  @spec try_to_recover(State.t()) :: State.t()
  def try_to_recover(t), do: t

  @spec setup_for_initial_recovery(State.t()) :: State.t()
  def setup_for_initial_recovery(t) do
    t
    |> Map.put(:state, :recovery)
    |> Map.put(
      :recovery_attempt,
      RecoveryAttempt.new(
        t.cluster,
        t.epoch,
        now()
      )
    )
  end

  @spec setup_for_subsequent_recovery(State.t()) :: State.t()
  def setup_for_subsequent_recovery(t) do
    t
    |> Map.update!(:recovery_attempt, fn recovery_attempt ->
      %{
        recovery_attempt
        | attempt: recovery_attempt.attempt + 1,
          state: :start
      }
    end)
  end

  @spec do_recovery(State.t()) :: State.t()
  def do_recovery(t) do
    trace_recovery_attempt_started(
      t.cluster,
      t.epoch,
      t.recovery_attempt.attempt,
      t.recovery_attempt.started_at
    )

    context = %{
      cluster_config: t.config,
      old_transaction_system_layout: t.old_transaction_system_layout,
      node_tracking: t.node_tracking,
      lock_token: t.lock_token,
      available_services: t.services,
      coordinator: t.coordinator
    }

    # Trace what services the director has when starting recovery
    old_logs = t.old_transaction_system_layout |> Map.get(:logs, %{}) |> Map.keys()
    available_service_ids = t.services |> Map.keys()

    trace_recovery_service_availability(old_logs, available_service_ids, t.services)

    t.recovery_attempt
    |> run_recovery_attempt(context)
    |> case do
      {:ok, completed} ->
        IO.inspect(completed, label: "Recovery completed")
        trace_recovery_completed(Interval.between(completed.started_at, now()))

        t
        |> Map.put(:state, :running)
        |> Map.update!(:config, fn config ->
          config
          |> Map.delete(:recovery_attempt)
        end)
        |> Map.put(:transaction_system_layout, completed.transaction_system_layout)
        |> persist_config()
        |> persist_new_transaction_system_layout()

      {{:stalled, reason}, stalled} ->
        trace_recovery_stalled(Interval.between(stalled.started_at, now()), reason)

        t
        |> Map.update!(:config, fn config ->
          config
          |> Map.put(:recovery_attempt, stalled)
        end)
        |> persist_config()
    end
  end

  @spec persist_config(State.t()) :: State.t()
  def persist_config(t) do
    case Coordinator.update_config(t.coordinator, t.config) do
      {:ok, txn_id} ->
        trace_recovery_attempt_persisted(txn_id)
        t

      {:error, reason} ->
        trace_recovery_attempt_persist_failed(reason)
        t
    end
  end

  @spec persist_new_transaction_system_layout(State.t()) :: State.t()
  def persist_new_transaction_system_layout(t) do
    case Coordinator.update_transaction_system_layout(t.coordinator, t.transaction_system_layout) do
      {:ok, txn_id} ->
        trace_recovery_layout_persisted(txn_id)
        t

      {:error, reason} ->
        trace_recovery_layout_persist_failed(reason)
        t
    end
  end

  @spec run_recovery_attempt(RecoveryAttempt.t(), recovery_context()) ::
          {:ok, RecoveryAttempt.t()}
          | {{:stalled, RecoveryAttempt.reason_for_stall()}, RecoveryAttempt.t()}
          | {:error, {:unexpected_recovery_state, atom()}}
  def run_recovery_attempt(t, context) do
    case t.state do
      {:stalled, reason} ->
        {{:stalled, reason}, t}

      :completed ->
        {:ok, t}

      state ->
        phase = next_phase(state)

        case phase.execute(t, context) do
          %{state: :completed} = t ->
            {:ok, t}

          %{state: {:stalled, _reason} = stalled} = t ->
            {stalled, t}

          %{state: new_state} = new_t when t.state != new_state ->
            new_t |> run_recovery_attempt(context)

          # Catch any unexpected states
          %{state: unexpected_state} = recovery_state ->
            trace_recovery_unexpected_state(unexpected_state, recovery_state)
            {:error, {:unexpected_recovery_state, unexpected_state}}
        end
    end
  end

  @spec next_phase(RecoveryAttempt.state()) :: module()
  defp next_phase(:start), do: __MODULE__.StartPhase
  defp next_phase(:lock_old_system_services), do: __MODULE__.LockOldSystemServicesPhase
  defp next_phase(:first_time_initialization), do: __MODULE__.InitializationPhase
  defp next_phase(:determine_old_logs_to_copy), do: __MODULE__.LogDiscoveryPhase
  defp next_phase(:create_vacancies), do: __MODULE__.VacancyCreationPhase
  defp next_phase(:determine_durable_version), do: __MODULE__.DurableVersionPhase
  defp next_phase(:recruit_logs_to_fill_vacancies), do: __MODULE__.RecruitLogsToFillVacanciesPhase
  defp next_phase(:recruit_storage_to_fill_vacancies), do: __MODULE__.StorageRecruitmentPhase
  defp next_phase(:replay_old_logs), do: __MODULE__.LogReplayPhase
  defp next_phase(:repair_data_distribution), do: __MODULE__.DataDistributionPhase
  defp next_phase(:define_sequencer), do: __MODULE__.SequencerPhase
  defp next_phase(:define_commit_proxies), do: __MODULE__.CommitProxyPhase
  defp next_phase(:define_resolvers), do: __MODULE__.ResolverPhase
  defp next_phase(:final_checks), do: __MODULE__.ValidationPhase
  defp next_phase(:persist_system_state), do: __MODULE__.PersistencePhase
  defp next_phase(:monitor_components), do: __MODULE__.MonitoringPhase
  defp next_phase(:cleanup_obsolete_workers), do: __MODULE__.WorkerCleanupPhase
end
