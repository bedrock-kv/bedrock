defmodule Bedrock.ControlPlane.Director.Recovery do
  @moduledoc """
  Orchestrates distributed system recovery through a coordinated phase sequence.

  This module implements Bedrock's recovery orchestration, which rebuilds the
  transaction system after critical component failures. Recovery follows a
  linear state machine where each phase either transitions to the next phase
  or stalls pending resource availability.

  The process begins by attempting to lock services from the previous transaction
  system layout, then branches into either first-time initialization or recovery
  from existing persistent state. Each phase validates its prerequisites and
  may stall if conditions are not met, with retry logic triggered when the
  environment changes.

  Recovery attempts are persisted at major milestones, allowing resumption from
  consistent checkpoints if interrupted. The orchestrator coordinates between
  phases but delegates specific recovery logic to individual phase modules.

  Critical components that trigger recovery include coordinators, directors,
  sequencers, commit proxies, resolvers, and transaction logs. Storage servers
  and gateways handle failures independently without triggering full recovery.

  See `Bedrock.ControlPlane.Director` for epoch management and
  `Bedrock.ControlPlane.Director.Nodes` for service discovery integration.
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.Internal.Time.Interval
  alias Bedrock.Service.Worker

  import Bedrock.Internal.Time, only: [now: 0]

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @type recovery_context :: %{
          cluster_config: Config.t(),
          old_transaction_system_layout: TransactionSystemLayout.t(),
          node_capabilities: %{Bedrock.Cluster.capability() => [node()]},
          lock_token: binary(),
          available_services: %{Worker.id() => {atom(), {atom(), node()}}},
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
        | attempt: recovery_attempt.attempt + 1
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
      node_capabilities: t.node_capabilities,
      lock_token: t.lock_token,
      available_services: t.services,
      coordinator: t.coordinator
    }

    old_logs = t.old_transaction_system_layout |> Map.get(:logs, %{}) |> Map.keys()
    available_service_ids = t.services |> Map.keys()

    trace_recovery_service_availability(old_logs, available_service_ids, t.services)

    t.recovery_attempt
    |> run_recovery_attempt(context)
    |> case do
      {:ok, completed} ->
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

  @spec run_recovery_attempt(RecoveryAttempt.t(), recovery_context(), module()) ::
          {:ok, RecoveryAttempt.t()}
          | {{:stalled, RecoveryAttempt.reason_for_stall()}, RecoveryAttempt.t()}
  def run_recovery_attempt(t, context, next_phase_module \\ __MODULE__.StartupPhase) do
    case next_phase_module.execute(t, context) do
      {completed_attempt, :completed} ->
        {:ok, completed_attempt}

      {stalled_attempt, {:stalled, _reason} = stalled} ->
        {stalled, stalled_attempt}

      {updated_attempt, next_next_phase_module} ->
        updated_attempt |> run_recovery_attempt(context, next_next_phase_module)
    end
  end
end
