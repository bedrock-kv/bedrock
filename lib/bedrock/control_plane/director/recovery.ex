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

  import Bedrock.ControlPlane.Director.Recovery.Telemetry
  import Bedrock.Internal.Time, only: [now: 0]

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.Internal.Time.Interval
  alias Bedrock.Service.Worker

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
    Map.update!(t, :recovery_attempt, fn recovery_attempt ->
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

    # Refresh the service view from the coordinator: workers register as
    # they come up on a booting node, and the snapshot this director was
    # started with goes stale immediately. Without the refresh, a restart's
    # materializers are invisible to recovery and their durable state is
    # orphaned. On any failure, fall back to what we already know.
    t = %{t | services: refresh_available_services(t)}

    context = %{
      cluster_config: t.config,
      old_transaction_system_layout: t.old_transaction_system_layout,
      node_capabilities: t.node_capabilities,
      lock_token: t.lock_token,
      available_services: t.services,
      coordinator: t.coordinator
    }

    t.recovery_attempt
    |> run_recovery_attempt(context)
    |> case do
      {:ok, completed} ->
        trace_recovery_completed(Interval.between(completed.started_at, now(), :microsecond))

        t
        |> Map.put(:state, :running)
        |> Map.update!(:config, fn config ->
          Map.delete(config, :recovery_attempt)
        end)
        |> Map.put(:transaction_system_layout, completed.transaction_system_layout)
        |> persist_config()
        |> persist_new_transaction_system_layout()
        |> prune_service_directory(completed)

      {{:stalled, reason}, stalled} ->
        trace_recovery_stalled(Interval.between(stalled.started_at, now()), reason)

        # The live state adopts the stalled attempt too — the persisted
        # config and the in-memory attempt must be the same logical
        # attempt. The next in-process retry builds on it; leaving the
        # older attempt in memory would discard the phases' accumulated
        # observations (lock-failed ids, recruited services) and redo —
        # or worse, repeat — that work every retry.
        t
        |> Map.put(:recovery_attempt, stalled)
        |> Map.update!(:config, fn config ->
          Map.put(config, :recovery_attempt, stalled)
        end)
        |> persist_config()

      {{:error, reason}, _failed_attempt} ->
        # Errors are fatal - this director should stop trying to recover
        trace_recovery_failed(Interval.between(t.recovery_attempt.started_at, now()), reason)
        t
    end
  end

  @doc """
  The directory ids a completed recovery's layout does not reference.

  These are ghosts: registrations left behind by workers on nodes that no
  longer exist under that name (node names change across restarts, and
  nothing on a dead node can deregister itself). Entries on live nodes need
  no help here — displaced workers self-retire on the layout push and their
  foreman deregisters them — but only the director can clean up for the
  dead.
  """
  @spec ghost_directory_ids(
          services :: %{Worker.id() => term()},
          RecoveryAttempt.t()
        ) :: [Worker.id()]
  def ghost_directory_ids(services, completed_attempt) do
    referenced = layout_reference_ids(completed_attempt)

    services
    |> Map.keys()
    |> Enum.reject(&MapSet.member?(referenced, &1))
  end

  # The completed layout's statement of what exists: the new-generation
  # logs and the active shard materializers — computed from the attempt
  # (the TSL carries no membership map). Ghost pruning treats anything
  # the completed recovery does not reference as a candidate ghost.
  @spec layout_reference_ids(RecoveryAttempt.t()) :: MapSet.t(Worker.id())
  defp layout_reference_ids(completed_attempt) do
    log_ids = completed_attempt.logs |> Map.keys() |> MapSet.new()

    materializer_ids =
      for {_tag, {worker_id, _node}} <- completed_attempt.shard_materializers,
          into: MapSet.new(),
          do: worker_id

    MapSet.union(log_ids, materializer_ids)
  end

  @spec prune_service_directory(State.t(), RecoveryAttempt.t()) :: State.t()
  defp prune_service_directory(t, completed_attempt) do
    case ghost_directory_ids(t.services, completed_attempt) do
      [] ->
        t

      ghost_ids ->
        _ = Coordinator.deregister_services(t.coordinator, ghost_ids)
        %{t | services: Map.drop(t.services, ghost_ids)}
    end
  catch
    :exit, _ -> t
  end

  @spec refresh_available_services(State.t()) :: %{Worker.id() => {atom(), {atom(), node()}}}
  defp refresh_available_services(t) do
    # Services that failed to lock in an earlier attempt of this recovery
    # stay excluded: the failed lock was this director's own observation,
    # and relearning it every attempt would re-pay the replacement work
    # each time.
    remembered_failures =
      Map.get(t.recovery_attempt || %{}, :lock_failed_service_ids) || MapSet.new()

    case Coordinator.fetch_service_directory(t.coordinator, 2_000) do
      {:ok, directory} -> t.services |> Map.merge(directory) |> Map.drop(MapSet.to_list(remembered_failures))
      _ -> Map.drop(t.services, MapSet.to_list(remembered_failures))
    end
  catch
    :exit, _ -> t.services
  end

  @spec persist_config(State.t()) :: State.t()
  def persist_config(t) do
    # Notify coordinator of config update directly (no Raft consensus).
    # Config is persisted to object storage by the persistence phase.
    Coordinator.notify_config(t.coordinator, t.config)
    trace_recovery_attempt_persisted(:notified)
    t
  end

  @spec persist_new_transaction_system_layout(State.t()) :: State.t()
  def persist_new_transaction_system_layout(t) do
    # Notify coordinator of new TSL directly (no Raft consensus).
    # TSL is already persisted to object storage by the persistence phase.
    Coordinator.notify_transaction_system_layout(t.coordinator, t.transaction_system_layout)
    trace_recovery_layout_persisted(:notified)
    t
  end

  @spec run_recovery_attempt(RecoveryAttempt.t(), recovery_context(), module()) ::
          {:ok, RecoveryAttempt.t()}
          | {{:stalled, RecoveryAttempt.reason_for_stall()}, RecoveryAttempt.t()}
          | {{:error, RecoveryAttempt.reason_for_stall()}, RecoveryAttempt.t()}
  def run_recovery_attempt(t, context, next_phase_module \\ __MODULE__.TSLValidationPhase) do
    case next_phase_module.execute(t, context) do
      {completed_attempt, :completed} ->
        {:ok, completed_attempt}

      {stalled_attempt, {:error, _reason} = error} ->
        {error, stalled_attempt}

      {stalled_attempt, {:stalled, _reason} = stalled} ->
        {stalled, stalled_attempt}

      {updated_attempt, next_next_phase_module} ->
        run_recovery_attempt(updated_attempt, context, next_next_phase_module)
    end
  end
end
