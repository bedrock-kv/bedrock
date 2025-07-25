defmodule Bedrock.ControlPlane.Director.Recovery do
  @moduledoc false

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Director.NodeTracking
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.Internal.Time.Interval
  alias Bedrock.Service.Worker

  require Logger

  import Bedrock.Internal.Time, only: [now: 0]

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @type recovery_context :: %{
          cluster_config: Config.t(),
          node_tracking: NodeTracking.t(),
          lock_token: binary(),
          available_services: %{Worker.id() => ServiceDescriptor.t()},
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
      node_tracking: t.node_tracking,
      lock_token: t.lock_token,
      available_services: t.services,
      coordinator: t.coordinator
    }

    t.recovery_attempt
    |> run_recovery_attempt(context)
    |> case do
      {:ok, completed} ->
        trace_recovery_completed(Interval.between(completed.started_at, now()))

        t
        |> Map.put(:state, :running)
        |> Map.put(:config, completed.cluster_config)

      {{:stalled, reason}, stalled} ->
        trace_recovery_stalled(Interval.between(stalled.started_at, now()), reason)

        t
        |> Map.update!(:config, fn config ->
          config
          |> Map.put(:recovery_attempt, stalled)
        end)
    end
    |> persist_recovery_attempt()
  end

  @spec persist_recovery_attempt(State.t()) :: State.t()
  def persist_recovery_attempt(t) do
    case Coordinator.write_config(t.coordinator, t.config) do
      {:ok, txn_id} ->
        Logger.info("Recovery attempt persisted with txn ID: #{inspect(txn_id)}")
        t

      {:error, reason} ->
        Logger.error("Failed to persist recovery attempt: #{inspect(reason)}")
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
        Logger.warning("Recovery is stalled: #{inspect(reason)}")
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
            Logger.error("Recovery attempt in unexpected state: #{inspect(unexpected_state)}")
            Logger.debug("Full recovery state: #{inspect(recovery_state)}")
            {:error, {:unexpected_recovery_state, unexpected_state}}
        end
    end
  end

  @spec next_phase(RecoveryAttempt.state()) :: module()
  defp next_phase(:start), do: __MODULE__.StartPhase
  defp next_phase(:lock_available_services), do: __MODULE__.LockServicesPhase
  defp next_phase(:first_time_initialization), do: __MODULE__.InitializationPhase
  defp next_phase(:determine_old_logs_to_copy), do: __MODULE__.LogDiscoveryPhase
  defp next_phase(:create_vacancies), do: __MODULE__.VacancyCreationPhase
  defp next_phase(:determine_durable_version), do: __MODULE__.DurableVersionPhase
  defp next_phase(:recruit_logs_to_fill_vacancies), do: __MODULE__.LogRecruitmentPhase
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
