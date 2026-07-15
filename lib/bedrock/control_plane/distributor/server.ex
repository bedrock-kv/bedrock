defmodule Bedrock.ControlPlane.Distributor.Server do
  @moduledoc """
  GenServer implementation of the Distributor singleton.

  Holds the epoch it was recruited under, a reference to the recruiting
  director, and a snapshot of the shard layout. Monitors the director and
  stops (`:normal`) when the director exits or when a newer epoch is
  signaled - the next director recruits a fresh distributor.

  Owns Phase A coverage policy: placeholder supervision, on-demand
  materializer recruitment, and materializer-death healing (monitor the
  shard's materializers; on death swap the placeholder into the TSL slot
  and re-recruit). Every shard-map mutation passes through the epoch
  guard implemented here.
  """

  use GenServer

  import Bedrock.ControlPlane.Distributor.Telemetry,
    only: [
      emit_distributor_started: 3,
      emit_distributor_stopped: 3,
      emit_coverage_sweep: 3,
      emit_coverage_demand: 2,
      emit_recruitment_started: 3,
      emit_recruitment_succeeded: 5,
      emit_recruitment_failed: 5,
      emit_materializer_down: 4,
      emit_healing_started: 3,
      emit_healing_completed: 3
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.ControlPlane.Distributor.Recruitment
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Version

  require Logger

  @default_backoff_ms 5_000

  @doc false
  @spec child_spec(
          opts :: [
            cluster: module(),
            epoch: Bedrock.epoch(),
            director: pid(),
            shard_layout: TransactionSystemLayout.shard_layout(),
            transaction_system_layout: TransactionSystemLayout.t(),
            durable_version: Bedrock.version(),
            node_capabilities: %{Bedrock.Cluster.capability() => [node()]},
            backoff_ms: pos_integer(),
            recruitment: map(),
            otp_name: atom()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    state = initial_state(opts)

    %{
      id: {__MODULE__, state.cluster, state.epoch},
      start: {GenServer, :start_link, [__MODULE__, state, [name: otp_name]]},
      restart: :temporary
    }
  end

  @spec initial_state(keyword()) :: State.t()
  defp initial_state(opts) do
    %State{
      cluster: Keyword.fetch!(opts, :cluster),
      epoch: Keyword.fetch!(opts, :epoch),
      director: Keyword.fetch!(opts, :director),
      shard_layout: Keyword.get(opts, :shard_layout, %{}),
      transaction_system_layout: Keyword.get(opts, :transaction_system_layout, %{}),
      durable_version: Keyword.get_lazy(opts, :durable_version, &Version.zero/0),
      node_capabilities: Keyword.get(opts, :node_capabilities, %{}),
      backoff_ms: Keyword.get(opts, :backoff_ms, @default_backoff_ms),
      recruitment_overrides: Keyword.get(opts, :recruitment, %{})
    }
  end

  @impl true
  @spec init(State.t()) :: {:ok, State.t(), {:continue, :coverage_sweep}}
  def init(%State{} = state) do
    # Monitor the Director - if it dies, this distributor should terminate
    # and let the next director recruit a fresh one. Trap exits so the
    # linked placeholder can be restarted when it dies.
    Process.flag(:trap_exit, true)
    Process.monitor(state.director)

    emit_distributor_started(state.cluster, state.epoch, state.director)

    {:ok, state |> start_placeholder() |> monitor_existing_materializers(), {:continue, :coverage_sweep}}
  end

  # The distributor's first act: diff the shard layout against the
  # shard_materializers snapshot recovery produced and publish the
  # placeholder pid into every uncovered slot (one batched TSL delta), so
  # clients route to the placeholder instead of hitting the LayoutIndex
  # loud-failure backstop. Recruitment stays demand-driven: it is triggered
  # by a real read arriving at the placeholder, never by the sweep.
  @impl true
  def handle_continue(:coverage_sweep, %State{} = t) do
    uncovered = uncovered_tags(t)
    emit_coverage_sweep(t.cluster, t.epoch, MapSet.size(uncovered))

    %{t | placeholder_tags: MapSet.union(t.placeholder_tags, uncovered)}
    |> publish_placeholder(uncovered)
    |> noreply()
  end

  @impl true
  def handle_call({:check_epoch, epoch}, _from, %State{} = t) do
    case State.check_epoch(t, epoch) do
      :ok ->
        reply(t, :ok)

      {:error, :epoch_superseded} = error ->
        Logger.info("Distributor for epoch #{t.epoch} superseded by epoch #{epoch}; stopping")
        {:stop, :normal, error, t}

      {:error, :newer_epoch_exists} = error ->
        reply(t, error)
    end
  end

  @impl true
  def handle_cast({:epoch_changed, new_epoch}, %State{} = t) do
    case State.check_epoch(t, new_epoch) do
      {:error, :epoch_superseded} ->
        Logger.info("Distributor for epoch #{t.epoch} superseded by epoch #{new_epoch}; stopping")
        stop(t, :normal)

      {:error, :newer_epoch_exists} ->
        # A notification carrying an epoch *older* than our own should not
        # occur in a healthy cluster; log it as suspicious and ignore it.
        Logger.warning(
          "Distributor for epoch #{t.epoch} ignoring suspicious epoch change notification " <>
            "for older epoch #{new_epoch}"
        )

        noreply(t)

      :ok ->
        noreply(t)
    end
  end

  # Coverage demand from the placeholder: recruit a materializer for the
  # shard unless a recruitment is already in flight or the tag is inside
  # its failure-backoff window. A tag we already cover with a monitored,
  # live materializer is re-delivered instead of re-recruited: a restarted
  # placeholder loses its `covered` map, and the `notify_covered` for a
  # completed recruitment may have been cast to the dead placeholder pid.
  def handle_cast({:coverage_demand, tag}, %State{} = t) do
    Logger.info("Distributor for epoch #{t.epoch} received coverage demand for shard tag #{inspect(tag)}")
    emit_coverage_demand(t.cluster, tag)

    case covered_materializer(t, tag) do
      {:ok, materializer} ->
        if t.placeholder, do: Placeholder.notify_covered(t.placeholder, tag, materializer)
        noreply(t)

      :error ->
        t
        |> maybe_recruit(tag)
        |> noreply()
    end
  end

  # Completion of an asynchronous recruitment attempt (cast back to
  # ourselves from the recruitment task).
  def handle_cast({:recruitment_complete, tag, {:ok, materializer, node}, duration_us}, %State{} = t) do
    emit_recruitment_succeeded(t.cluster, t.epoch, tag, node, duration_us)
    if t.placeholder, do: Placeholder.notify_covered(t.placeholder, tag, materializer)

    # The recruited materializer now occupies the TSL slot, so a placeholder
    # restart must no longer republish the placeholder into it.
    t
    |> monitor_materializer(tag, materializer)
    |> complete_healing(tag)
    |> Map.update!(:pending_demands, &MapSet.delete(&1, tag))
    |> Map.update!(:placeholder_tags, &MapSet.delete(&1, tag))
    |> noreply()
  end

  def handle_cast({:recruitment_complete, tag, {:error, :newer_epoch_exists}, _duration_us}, %State{} = t) do
    Logger.info(
      "Distributor for epoch #{t.epoch} superseded (TSL delta for shard tag #{inspect(tag)} rejected); stopping"
    )

    stop(t, :normal)
  end

  def handle_cast({:recruitment_complete, tag, {:error, reason}, duration_us}, %State{} = t) do
    t
    |> recruitment_failed(tag, reason, duration_us)
    |> noreply()
  end

  # Completion of an asynchronous coverage-sweep publication. A rejection
  # on epoch grounds means this distributor has been superseded, mirroring
  # check_epoch semantics: stop and cede to the new epoch's distributor.
  def handle_cast({:sweep_complete, _tags, :ok}, %State{} = t), do: noreply(t)

  def handle_cast({:sweep_complete, _tags, {:error, :newer_epoch_exists}}, %State{} = t) do
    Logger.info("Distributor for epoch #{t.epoch} superseded (coverage sweep delta rejected); stopping")

    stop(t, :normal)
  end

  def handle_cast({:sweep_complete, tags, {:error, reason}}, %State{} = t) do
    # The LayoutIndex loud-failure backstop still covers these slots, and a
    # placeholder restart republishes them, so degrade with a warning only.
    Logger.warning(
      "Distributor for epoch #{t.epoch} failed to publish placeholder coverage for shard tags " <>
        "#{inspect(tags)}: #{inspect(reason)}"
    )

    noreply(t)
  end

  # Internal/test seam: relay a coverage result to the placeholder.
  def handle_cast({:deliver_coverage, tag, materializer}, %State{} = t) do
    if t.placeholder, do: Placeholder.notify_covered(t.placeholder, tag, materializer)

    noreply(%{t | pending_demands: MapSet.delete(t.pending_demands, tag)})
  end

  def handle_cast({:fail_coverage, tag, reason}, %State{} = t) do
    if t.placeholder, do: Placeholder.notify_coverage_failed(t.placeholder, tag, reason)

    noreply(%{t | pending_demands: MapSet.delete(t.pending_demands, tag)})
  end

  # Outcome of the placeholder swap a materializer death triggered. On
  # success, re-recruit eagerly: the shard demonstrably had traffic (it was
  # materialized), so healing should not wait for the next read; the
  # per-tag failure backoff in `maybe_recruit/2` still applies. Chaining
  # the recruitment after the swap also guarantees the recruitment's TSL
  # delta lands after the placeholder's.
  def handle_cast({:placeholder_swap_complete, tag, :ok}, %State{} = t) do
    t
    |> maybe_recruit(tag)
    |> noreply()
  end

  def handle_cast({:placeholder_swap_complete, tag, {:error, :newer_epoch_exists}}, %State{} = t) do
    Logger.info(
      "Distributor for epoch #{t.epoch} superseded (placeholder swap for shard tag #{inspect(tag)} rejected); stopping"
    )

    stop(t, :normal)
  end

  def handle_cast({:placeholder_swap_complete, tag, error}, %State{} = t) do
    Logger.warning(
      "Distributor for epoch #{t.epoch} failed to swap the placeholder into shard tag " <>
        "#{inspect(tag)}: #{inspect(error)}"
    )

    # Recruit anyway: a successful recruitment writes its own TSL delta,
    # which heals the slot without the intermediate placeholder hop.
    t
    |> maybe_recruit(tag)
    |> noreply()
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, director, reason}, %State{director: director} = t) do
    Logger.info("Distributor for epoch #{t.epoch} stopping: director exited (#{inspect(reason)})")

    stop(t, :normal)
  end

  # A monitored materializer died: heal the shard. The placeholder is told
  # the tag is uncovered (so requests park and re-demand instead of being
  # forwarded to the corpse), the placeholder pid is swapped into the TSL
  # slot, and - once the swap completes - the tag is eagerly re-recruited.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = t) when is_map_key(t.materializer_monitors, ref) do
    {{tag, _pid}, monitors} = Map.pop(t.materializer_monitors, ref)

    Logger.warning(
      "Distributor for epoch #{t.epoch} healing shard tag #{inspect(tag)}: materializer exited (#{inspect(reason)})"
    )

    if t.placeholder, do: Placeholder.notify_uncovered(t.placeholder, tag)
    emit_materializer_down(t.cluster, t.epoch, tag, reason)
    emit_healing_started(t.cluster, t.epoch, tag)

    # A healed tag is placeholder-covered: track it in placeholder_tags so a
    # placeholder restart republishes the new pid into its slot.
    %{
      t
      | materializer_monitors: monitors,
        healing: MapSet.put(t.healing, tag),
        placeholder_tags: MapSet.put(t.placeholder_tags, tag)
    }
    |> swap_placeholder_into_slot(tag)
    |> noreply()
  end

  def handle_info({:EXIT, placeholder, reason}, %State{placeholder: placeholder} = t) do
    Logger.warning("Distributor for epoch #{t.epoch} restarting placeholder (exited: #{inspect(reason)})")

    # The old pid is stale in every TSL slot it occupied - swept, and slots
    # healed onto it after a materializer death: republish the new
    # placeholder pid into those slots so clients keep routing somewhere live.
    # Tags with a recruitment in flight are excluded: the recruitment's real
    # pid may already occupy the slot (its delta is applied before its
    # completion cast is processed here) and must never regress to the
    # placeholder. If the recruitment fails instead, recruitment_failed/4
    # re-asserts the live placeholder for the slot.
    restarted = start_placeholder(t)

    restarted
    |> publish_placeholder(MapSet.difference(restarted.placeholder_tags, restarted.pending_demands))
    |> noreply()
  end

  def handle_info(message, %State{} = t) do
    Logger.debug("Distributor ignoring unexpected message: #{inspect(message)}")
    noreply(t)
  end

  @impl true
  def terminate(reason, %State{} = t) do
    # A `:normal` exit signal would be ignored by the linked placeholder,
    # so shut it down explicitly to avoid orphaning it.
    if t.placeholder && Process.alive?(t.placeholder), do: Process.exit(t.placeholder, :shutdown)

    emit_distributor_stopped(t.cluster, t.epoch, reason)
    :ok
  end

  # Coverage sweep

  # Shard tags in the layout with no materializer in the TSL snapshot the
  # director handed us at recruitment time.
  @spec uncovered_tags(State.t()) :: MapSet.t(Bedrock.range_tag())
  defp uncovered_tags(%State{} = t) do
    covered = covered_tags(t.transaction_system_layout)

    t.shard_layout
    |> Map.values()
    |> MapSet.new(fn {tag, _start_key} -> tag end)
    |> MapSet.difference(covered)
  end

  # Mirrors the client's LayoutIndex routing: data shards are covered by a
  # shard_materializers entry; the system shard is covered by the metadata
  # materializer.
  @spec covered_tags(TransactionSystemLayout.t() | %{}) :: MapSet.t(Bedrock.range_tag())
  defp covered_tags(transaction_system_layout) do
    covered =
      transaction_system_layout
      |> Map.get(:shard_materializers)
      |> Kernel.||(%{})
      |> Map.keys()
      |> MapSet.new()

    case Map.get(transaction_system_layout, :metadata_materializer) do
      pid when is_pid(pid) -> MapSet.put(covered, RecoveryAttempt.system_shard_id())
      _ -> covered
    end
  end

  # Publishes the current placeholder pid into the given tags' TSL slots as
  # a single batched delta. Runs off the message loop (an in-line call from
  # init/handle_continue would deadlock against the director, which blocks
  # on check_epoch right after starting us); the outcome is cast back
  # through `completion` so state mutation stays serialized. The sweep and
  # restart-republish paths report as {:sweep_complete, tags, result}; the
  # death-healing swap reports as {:placeholder_swap_complete, tag, result},
  # whose handler chains the per-tag eager re-recruitment after the swap's
  # delta has landed.
  @spec publish_placeholder(State.t(), MapSet.t(Bedrock.range_tag())) :: State.t()
  defp publish_placeholder(%State{} = t, tags),
    do: publish_placeholder(t, tags, &{:sweep_complete, MapSet.to_list(tags), &1})

  @spec publish_placeholder(State.t(), MapSet.t(Bedrock.range_tag()), (result :: term() -> message :: term())) ::
          State.t()
  defp publish_placeholder(%State{} = t, tags, completion) do
    if MapSet.size(tags) == 0 do
      t
    else
      delta = Map.new(tags, &{&1, t.placeholder})
      distributor = self()
      director = t.director
      epoch = t.epoch

      {:ok, _pid} =
        Task.start(fn ->
          result =
            try do
              Director.apply_tsl_delta(director, delta, epoch)
            rescue
              exception -> {:error, {:placeholder_publish_crashed, Exception.message(exception)}}
            catch
              :exit, reason -> {:error, {:placeholder_publish_exited, reason}}
            end

          GenServer.cast(distributor, completion.(result))
        end)

      t
    end
  end

  # Recruitment

  @spec maybe_recruit(State.t(), Bedrock.range_tag()) :: State.t()
  defp maybe_recruit(%State{} = t, tag) do
    cond do
      match?({:ok, _}, covered_materializer(t, tag)) ->
        # Already covered by a live monitored materializer: a demand-raced
        # recruitment completed while a placeholder swap was in flight.
        # Recruiting again would orphan the live recruit.
        t

      MapSet.member?(t.pending_demands, tag) ->
        # A recruitment for this tag is already in flight.
        t

      in_backoff?(t, tag) ->
        Logger.debug("Distributor for epoch #{t.epoch} ignoring demand for shard tag #{inspect(tag)} (in backoff)")

        # Shed promptly and clear the placeholder's demand dedupe: parked
        # readers would otherwise wait out their full budget, and a stuck
        # `demanded` flag would suppress the re-demand that retries
        # recruitment once the backoff expires.
        if t.placeholder, do: Placeholder.notify_coverage_failed(t.placeholder, tag, :backoff)
        t

      true ->
        start_recruitment(%{t | backoff: Map.delete(t.backoff, tag)}, tag)
    end
  end

  @spec in_backoff?(State.t(), Bedrock.range_tag()) :: boolean()
  defp in_backoff?(%State{backoff: backoff}, tag) do
    case Map.fetch(backoff, tag) do
      {:ok, expires_at_ms} -> System.monotonic_time(:millisecond) < expires_at_ms
      :error -> false
    end
  end

  @spec start_recruitment(State.t(), Bedrock.range_tag()) :: State.t()
  defp start_recruitment(%State{} = t, tag) do
    case shard_range(t.shard_layout, tag) do
      {:ok, _range} ->
        emit_recruitment_started(t.cluster, t.epoch, tag)
        spawn_recruitment_task(t, tag)
        %{t | pending_demands: MapSet.put(t.pending_demands, tag)}

      :error ->
        recruitment_failed(t, tag, :unknown_shard_tag, 0)
    end
  end

  # Resolves the key range for a shard tag from the layout snapshot
  # (keyed by end_key, valued by {tag, start_key}).
  @spec shard_range(TransactionSystemLayout.shard_layout(), Bedrock.range_tag()) ::
          {:ok, Bedrock.key_range()} | :error
  defp shard_range(shard_layout, tag) do
    Enum.find_value(shard_layout, :error, fn {end_key, {shard_tag, start_key}} ->
      if shard_tag == tag, do: {:ok, {start_key, end_key}}
    end)
  end

  # Runs the recruitment (foreman worker creation, epoch lock/unlock, and
  # the TSL delta to the director) off the distributor's message loop, then
  # casts the outcome back so state mutation stays serialized.
  @spec spawn_recruitment_task(State.t(), Bedrock.range_tag()) :: :ok
  defp spawn_recruitment_task(%State{} = t, tag) do
    distributor = self()
    director = t.director
    epoch = t.epoch
    context = recruitment_context(t)

    {:ok, _pid} =
      Task.start(fn ->
        started_at = System.monotonic_time(:microsecond)

        result =
          try do
            with {:ok, materializer, node, worker_id} <- Recruitment.recruit(tag, context) do
              case Director.apply_tsl_delta(director, %{tag => materializer}, epoch) do
                :ok ->
                  {:ok, materializer, node}

                error ->
                  # The recruit is unfenced: no layout slot will ever name
                  # it, so nothing can read from it or heal it. Remove it
                  # rather than leak an orphaned worker.
                  Recruitment.remove_orphaned_worker(worker_id, node, context)
                  error
              end
            end
          rescue
            exception -> {:error, {:recruitment_crashed, Exception.message(exception)}}
          catch
            :exit, reason -> {:error, {:recruitment_exited, reason}}
          end

        duration_us = System.monotonic_time(:microsecond) - started_at
        GenServer.cast(distributor, {:recruitment_complete, tag, result, duration_us})
      end)

    :ok
  end

  @spec recruitment_context(State.t()) :: Recruitment.context()
  defp recruitment_context(%State{} = t) do
    Map.merge(
      %{
        cluster: t.cluster,
        epoch: t.epoch,
        durable_version: t.durable_version,
        transaction_system_layout: t.transaction_system_layout,
        node_capabilities: t.node_capabilities
      },
      t.recruitment_overrides
    )
  end

  @spec recruitment_failed(State.t(), Bedrock.range_tag(), reason :: term(), duration_us :: non_neg_integer()) ::
          State.t()
  defp recruitment_failed(%State{} = t, tag, reason, duration_us) do
    Logger.warning(
      "Distributor for epoch #{t.epoch} failed to recruit a materializer for shard tag " <>
        "#{inspect(tag)}: #{inspect(reason)}"
    )

    emit_recruitment_failed(t.cluster, t.epoch, tag, reason, duration_us)
    if t.placeholder, do: Placeholder.notify_coverage_failed(t.placeholder, tag, reason)

    updated = %{
      t
      | pending_demands: MapSet.delete(t.pending_demands, tag),
        backoff: Map.put(t.backoff, tag, System.monotonic_time(:millisecond) + t.backoff_ms)
    }

    # Re-assert the live placeholder for a placeholder-owned slot: a
    # placeholder restart during this recruitment skipped the tag in its
    # republish (see the :EXIT handler), so the slot may still hold the dead
    # pid. Otherwise this is an idempotent re-publication of the same pid.
    if MapSet.member?(updated.placeholder_tags, tag) do
      publish_placeholder(updated, MapSet.new([tag]))
    else
      updated
    end
  end

  # Death healing

  # Monitors the materializers the recovery-built TSL snapshot already
  # names: the director deliberately does not monitor materializers, so
  # without this the death of a bootstrap-created materializer would go
  # unhealed. The system shard is excluded - clients route it via the
  # TSL's `metadata_materializer` field, which a `shard_materializers`
  # delta cannot heal, so its death is a recovery-level concern.
  @system_shard RecoveryAttempt.system_shard_id()

  @spec monitor_existing_materializers(State.t()) :: State.t()
  defp monitor_existing_materializers(%State{} = t) do
    monitors =
      t.transaction_system_layout
      |> Map.get(:shard_materializers, %{})
      |> Enum.reduce(t.materializer_monitors, fn
        {tag, pid}, acc when is_pid(pid) and tag != @system_shard ->
          Map.put(acc, Process.monitor(pid), {tag, pid})

        _other, acc ->
          acc
      end)

    %{t | materializer_monitors: monitors}
  end

  # Monitors a newly recruited materializer, replacing any stale monitor
  # held for the same tag.
  @spec monitor_materializer(State.t(), Bedrock.range_tag(), pid()) :: State.t()
  defp monitor_materializer(%State{} = t, tag, materializer) do
    {stale, live} =
      Enum.split_with(t.materializer_monitors, fn {_ref, {monitored_tag, _pid}} -> monitored_tag == tag end)

    Enum.each(stale, fn {ref, _entry} -> Process.demonitor(ref, [:flush]) end)

    monitors = live |> Map.new() |> Map.put(Process.monitor(materializer), {tag, materializer})

    %{t | materializer_monitors: monitors}
  end

  @spec covered_materializer(State.t(), Bedrock.range_tag()) :: {:ok, pid()} | :error
  defp covered_materializer(%State{materializer_monitors: monitors}, tag) do
    Enum.find_value(monitors, :error, fn {_ref, {monitored_tag, pid}} ->
      if monitored_tag == tag, do: {:ok, pid}
    end)
  end

  @spec complete_healing(State.t(), Bedrock.range_tag()) :: State.t()
  defp complete_healing(%State{} = t, tag) do
    if MapSet.member?(t.healing, tag) do
      emit_healing_completed(t.cluster, t.epoch, tag)
      %{t | healing: MapSet.delete(t.healing, tag)}
    else
      t
    end
  end

  # Swaps the placeholder pid into a dead materializer's TSL slot so
  # stale-layout clients park at the placeholder instead of erroring
  # against a corpse. The delta is epoch-guarded at the director; the
  # {:placeholder_swap_complete, ...} outcome stops a superseded
  # distributor and serializes the eager re-recruitment after the swap.
  @spec swap_placeholder_into_slot(State.t(), Bedrock.range_tag()) :: State.t()
  defp swap_placeholder_into_slot(%State{} = t, tag),
    do: publish_placeholder(t, MapSet.new([tag]), &{:placeholder_swap_complete, tag, &1})

  # The placeholder is linked (so it dies with the distributor) and its
  # exit is caught via trap_exit above so the distributor can restart it.
  #
  # A restarted placeholder loses its `covered` and `demanded` state while
  # `pending_demands` here survives, so `maybe_recruit/2` tolerates
  # duplicate `{:coverage_demand, tag}` casts for tags already pending, and
  # the demand handler re-delivers coverage for tags with a live monitored
  # materializer (a `notify_covered` cast to the dead pid is silently
  # dropped).
  @spec start_placeholder(State.t()) :: State.t()
  defp start_placeholder(%State{} = t) do
    {:ok, placeholder} =
      GenServer.start_link(
        Placeholder.Server,
        {t.cluster, self(), t.shard_layout, Placeholder.default_hold_ms()}
      )

    %{t | placeholder: placeholder}
  end
end
