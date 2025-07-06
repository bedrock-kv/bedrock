defmodule Bedrock.ControlPlane.Director.Recovery do
  @moduledoc false

  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Internal.Time.Interval

  require Logger

  import Bedrock.Internal.Time, only: [now: 0]

  alias __MODULE__.StartPhase
  alias __MODULE__.LockServicesPhase
  alias __MODULE__.InitializationPhase
  alias __MODULE__.LogDiscoveryPhase
  alias __MODULE__.VacancyCreationPhase
  alias __MODULE__.DurableVersionPhase
  alias __MODULE__.LogRecruitmentPhase
  alias __MODULE__.StorageRecruitmentPhase
  alias __MODULE__.LogReplayPhase
  alias __MODULE__.DataDistributionPhase
  alias __MODULE__.SequencerPhase
  alias __MODULE__.CommitProxyPhase
  alias __MODULE__.ResolverPhase
  alias __MODULE__.ServiceCollectionPhase
  alias __MODULE__.ValidationPhase
  alias __MODULE__.PersistencePhase
  alias __MODULE__.MonitoringPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @spec try_to_recover(State.t()) :: State.t()
  def try_to_recover(%{state: :starting} = t) do
    t
    |> setup_for_initial_recovery()
    |> do_recovery()
  end

  def try_to_recover(%{state: :recovery} = t) do
    t
    |> setup_for_subsequent_recovery()
    |> do_recovery()
  end

  def try_to_recover(t), do: t

  def setup_for_initial_recovery(t) do
    t
    |> Map.put(:state, :recovery)
    |> Map.update!(:config, fn config ->
      config
      |> Map.put(
        :recovery_attempt,
        RecoveryAttempt.new(
          t.cluster,
          t.epoch,
          now(),
          config.coordinators,
          Map.take(config.parameters, [
            :desired_logs,
            :desired_replication_factor,
            :desired_commit_proxies
          ]),
          config.transaction_system_layout,
          t.services
        )
      )
      |> Map.update!(:transaction_system_layout, fn transaction_system_layout ->
        transaction_system_layout
        |> Map.put(:director, self())
        |> Map.put(:sequencer, nil)
        |> Map.put(:rate_keeper, nil)
        |> Map.put(:proxies, [])
        |> Map.put(:resolvers, [])
      end)
    end)
  end

  def setup_for_subsequent_recovery(t) do
    t
    |> Map.update!(:config, fn config ->
      config
      |> Map.update!(:recovery_attempt, fn recovery_attempt ->
        %{
          recovery_attempt
          | attempt: recovery_attempt.attempt + 1,
            state: :start,
            available_services: t.services
        }
      end)
    end)
  end

  @spec do_recovery(State.t()) :: State.t()
  def do_recovery(t) do
    trace_recovery_attempt_started(
      t.cluster,
      t.epoch,
      t.config.recovery_attempt.attempt,
      t.config.recovery_attempt.started_at
    )

    t.config.recovery_attempt
    |> run_recovery_attempt()
    |> case do
      {:ok, completed} ->
        trace_recovery_completed(Interval.between(completed.started_at, now()))

        t
        |> Map.put(:state, :running)
        |> Map.update!(:config, fn config ->
          config
          |> Map.delete(:recovery_attempt)
          |> Map.update!(:transaction_system_layout, fn transaction_system_layout ->
            transaction_system_layout
            |> Map.put(:id, TransactionSystemLayout.random_id())
            |> Map.put(:sequencer, completed.sequencer)
            |> Map.put(:resolvers, completed.resolvers)
            |> Map.put(:proxies, completed.proxies)
            |> Map.put(:logs, completed.logs)
            |> Map.put(:storage_teams, completed.storage_teams)
            |> Map.put(:services, completed.required_services)
          end)
        end)
        |> unlock_storage_after_recovery(completed.durable_version)

      {{:stalled, reason}, stalled} ->
        trace_recovery_stalled(Interval.between(stalled.started_at, now()), reason)

        t
        |> Map.update!(:config, fn config ->
          config
          |> Map.put(:recovery_attempt, stalled)
        end)
    end
  end

  def unlock_storage_after_recovery(t, durable_version) do
    t.config.transaction_system_layout.services
    |> Enum.each(fn
      {id, %{kind: :storage, status: {:up, worker}}} ->
        trace_recovery_storage_unlocking(id)

        Storage.unlock_after_recovery(worker, durable_version, t.config.transaction_system_layout,
          timeout_in_ms: 1_000
        )

      _ ->
        :ok
    end)

    t
  end

  @spec run_recovery_attempt(RecoveryAttempt.t()) ::
          {:ok, RecoveryAttempt.t()}
          | {{:stalled, RecoveryAttempt.reason_for_stall()}, RecoveryAttempt.t()}
          | {:error, term()}
  def run_recovery_attempt(t) do
    case recovery(t) do
      %{state: :completed} = t ->
        {:ok, t}

      %{state: {:stalled, _reason} = stalled} = t ->
        {stalled, t}

      %{state: new_state} = new_t when t.state != new_state ->
        new_t |> run_recovery_attempt()

      # Catch any unexpected states
      %{state: unexpected_state} = recovery_state ->
        Logger.error("Recovery attempt in unexpected state: #{inspect(unexpected_state)}")
        Logger.debug("Full recovery state: #{inspect(recovery_state)}")
        {:error, {:unexpected_recovery_state, unexpected_state}}
    end
  end

  @spec recovery(RecoveryAttempt.t()) :: RecoveryAttempt.t()
  def recovery(recovery_attempt)

  def recovery(%{state: :start} = recovery_attempt) do
    StartPhase.execute(recovery_attempt)
  end

  def recovery(%{state: :lock_available_services} = recovery_attempt),
    do: LockServicesPhase.execute(recovery_attempt)

  def recovery(%{state: :first_time_initialization} = recovery_attempt),
    do: InitializationPhase.execute(recovery_attempt)

  def recovery(%{state: :determine_old_logs_to_copy} = recovery_attempt),
    do: LogDiscoveryPhase.execute(recovery_attempt)

  def recovery(%{state: :create_vacancies} = recovery_attempt),
    do: VacancyCreationPhase.execute(recovery_attempt)

  def recovery(%{state: :determine_durable_version} = recovery_attempt),
    do: DurableVersionPhase.execute(recovery_attempt)

  def recovery(%{state: :recruit_logs_to_fill_vacancies} = recovery_attempt),
    do: LogRecruitmentPhase.execute(recovery_attempt)

  def recovery(%{state: :recruit_storage_to_fill_vacancies} = recovery_attempt),
    do: StorageRecruitmentPhase.execute(recovery_attempt)

  def recovery(%{state: :replay_old_logs} = recovery_attempt),
    do: LogReplayPhase.execute(recovery_attempt)

  def recovery(%{state: :repair_data_distribution} = recovery_attempt),
    do: DataDistributionPhase.execute(recovery_attempt)

  def recovery(%{state: :define_sequencer} = recovery_attempt),
    do: SequencerPhase.execute(recovery_attempt)

  def recovery(%{state: :define_commit_proxies} = recovery_attempt),
    do: CommitProxyPhase.execute(recovery_attempt)

  def recovery(%{state: :define_resolvers} = recovery_attempt),
    do: ResolverPhase.execute(recovery_attempt)

  def recovery(%{state: :define_required_services} = recovery_attempt),
    do: ServiceCollectionPhase.execute(recovery_attempt)

  def recovery(%{state: :final_checks} = recovery_attempt),
    do: ValidationPhase.execute(recovery_attempt)

  def recovery(%{state: :persist_system_state} = recovery_attempt),
    do: PersistencePhase.execute(recovery_attempt)

  def recovery(%{state: :monitor_components} = recovery_attempt),
    do: MonitoringPhase.execute(recovery_attempt)

  def recovery(%{state: {:stalled, reason}} = recovery_attempt) do
    Logger.warning("Recovery is stalled: #{inspect(reason)}")
    # Stay stalled - don't automatically retry
    recovery_attempt
  end

  def recovery(t), do: raise("Invalid state: #{inspect(t)}")
end
