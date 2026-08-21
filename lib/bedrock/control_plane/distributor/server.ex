defmodule Bedrock.ControlPlane.Distributor.Server do
  @moduledoc false
  use GenServer

  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.ControlPlane.Distributor.Recruitment
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.ControlPlane.Distributor.Telemetry
  alias Bedrock.ControlPlane.Distributor.Transactions
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  require Logger

  @placeholder_worker_id Placeholder.worker_id()

  # FDB's MOVEKEYS_LOCK_POLLING_DELAY: a superseded distributor exits
  # within seconds even when idle, instead of waiting to lose a commit.
  @poll_interval_ms 5_000

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start, [opts]},
      restart: :temporary
    }
  end

  @doc """
  Starts an unlinked distributor: the director supervises by monitor
  (ceded `:normal` exits are not re-recruited; failures are), and the
  distributor watches the director back — a per-epoch singleton dies
  with its epoch.
  """
  @spec start(keyword()) :: {:ok, pid()} | {:error, term()}
  def start(opts) do
    GenServer.start(__MODULE__, opts, name: Keyword.get(opts, :otp_name))
  end

  @impl true
  def init(opts) do
    cluster = Keyword.fetch!(opts, :cluster)
    epoch = Keyword.fetch!(opts, :epoch)
    director = Keyword.fetch!(opts, :director)

    deps =
      Keyword.get_lazy(opts, :deps, fn ->
        Transactions.deps_for(
          cluster,
          epoch,
          Keyword.fetch!(opts, :sequencer),
          Keyword.fetch!(opts, :proxies)
        )
      end)

    # The placeholder dies with the distributor and vice versa: they are
    # one coverage story, and the linked pair keeps the registered name
    # and the demand channel consistent.
    Process.flag(:trap_exit, true)

    state = %State{
      cluster: cluster,
      epoch: epoch,
      director: director,
      director_monitor: Process.monitor(director),
      deps: deps,
      poll_interval_ms: Keyword.get(opts, :poll_interval_ms, @poll_interval_ms),
      placeholder_start_fn: Keyword.get(opts, :placeholder_start_fn),
      recruitment_ctx: Keyword.get(opts, :recruitment_ctx),
      backoff_ms: Keyword.get(opts, :backoff_ms, 5_000)
    }

    {:ok, state, {:continue, :take_lock}}
  end

  # Lock first, everything else second (FDB's DD startup order): a
  # distributor that cannot own the fence must not exist. Take is
  # last-take-wins and never a supersession verdict (Transactions
  # retries aborts with fresh versions); any take failure stops
  # :shutdown so the director's retry recruits a fresh instance.
  # Supersession is delivered by the poll loop and, later, by the CHECK
  # fence on mutating transactions.
  @impl true
  def handle_continue(:take_lock, %State{} = t) do
    case Transactions.take_lock(t.deps) do
      {:ok, lock} ->
        Logger.info("Bedrock distributor (epoch #{t.epoch}): lock taken")
        {:noreply, schedule_poll(%{t | lock: lock}), {:continue, :startup_sweep}}

      {:error, reason} ->
        {:stop, {:shutdown, {:lock_take_failed, reason}}, t}
    end
  end

  # Lock, then snapshot, then work (FDB's DD startup order): read both
  # durable families at one pinned version, start the placeholder over
  # the read layout, and publish placeholder refs for every uncovered
  # tag in ONE check-fenced commit — coverage gaps become visible in the
  # keyspace, addressed to a ref that parks reads instead of failing
  # them. Supersession at the publish cedes; transient failures stop
  # :shutdown for the director's retry.
  def handle_continue(:startup_sweep, %State{} = t) do
    with {:ok, snapshot} <- Transactions.read_snapshot(t.deps),
         {:ok, placeholder} <- start_placeholder(t, snapshot.shard_layout),
         t = %{t | placeholder: placeholder, snapshot: snapshot},
         :ok <- publish_placeholders(t, uncovered_tags(snapshot)) do
      {:noreply, monitor_assignments(t)}
    else
      {:error, :superseded} ->
        Logger.info("Bedrock distributor (epoch #{t.epoch}): superseded at publish; ceding")
        {:stop, :normal, t}

      {:error, reason} ->
        {:stop, {:shutdown, {:startup_sweep_failed, reason}}, t}
    end
  end

  # Coverage demand from the placeholder: if the snapshot already names
  # a live assignment (a race between park and publish), hand the
  # placeholder the covered ref so it drains; otherwise recruit —
  # demand-driven, deduped by the in-flight set, damped by per-tag
  # backoff (the placeholder re-demands after a coverage_failed shed).
  @impl true
  def handle_cast({:coverage_demand, tag}, %State{} = t) do
    Telemetry.emit_coverage_demand(t.cluster, tag)

    case Map.fetch(t.snapshot.materializer_refs, tag) do
      {:ok, {worker_id, node}} when worker_id != @placeholder_worker_id ->
        Placeholder.notify_covered(t.placeholder, tag, callable_ref(t.cluster, worker_id, node))
        {:noreply, t}

      _uncovered_or_placeholder ->
        {:noreply, t |> Map.update!(:pending_demands, &MapSet.put(&1, tag)) |> maybe_recruit(tag)}
    end
  end

  # Recruitment runs in a task (the server must keep polling and
  # answering demand); the result serializes back through this message.
  # Success publishes the assignment under the CHECK fence from the
  # server process — one fence evaluator, no concurrent same-owner
  # commits from this distributor. An aborted-into-superseded publish
  # cedes AND removes the orphan (the recruit was never fenced into the
  # family; commit abort is the orphan-cleanup trigger).
  @impl true
  def handle_info({:recruitment_complete, tag, result}, %State{} = t) do
    t = %{t | recruiting: MapSet.delete(t.recruiting, tag)}
    # The task's DOWN (normal) follows; the ref entry is cleaned there.

    case result do
      {:ok, pid, node, worker_id} ->
        publish_recruit(t, tag, pid, node, worker_id)

      {:error, reason} ->
        Logger.warning("Bedrock distributor (epoch #{t.epoch}): recruitment for tag #{tag} failed: #{inspect(reason)}")
        Placeholder.notify_coverage_failed(t.placeholder, tag, reason)
        {:noreply, start_backoff(t, tag)}
    end
  end

  # The poll-to-die loop: a read-only fence evaluation every poll
  # interval. Supersession cedes; an unavailable read is not a verdict —
  # the next tick retries.
  @impl true
  def handle_info(:poll_lock, %State{} = t) do
    case Transactions.poll_verdict(t.lock, t.deps) do
      :superseded ->
        Logger.info("Bedrock distributor (epoch #{t.epoch}): lock superseded; ceding")
        {:stop, :normal, t}

      _ok_or_unavailable ->
        {:noreply, schedule_poll(t)}
    end
  end

  # A per-epoch singleton dies with its epoch: the director's death is a
  # recovery in progress, and the next epoch's director recruits the
  # next distributor.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{director_monitor: ref} = t), do: {:stop, :normal, t}

  # Recruit-task containment: a normal exit follows its completion
  # message (just clean the ref); an abnormal exit that beat any
  # completion synthesizes a failure so the tag leaves the in-flight set
  # and the placeholder can re-demand.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = t) when is_map_key(t.recruit_task_refs, ref) do
    {tag, refs} = Map.pop(t.recruit_task_refs, ref)
    t = %{t | recruit_task_refs: refs}

    if reason != :normal and MapSet.member?(t.recruiting, tag) do
      handle_info({:recruitment_complete, tag, {:error, {:recruit_task_crashed, reason}}}, t)
    else
      {:noreply, t}
    end
  end

  # Death healing (event-driven, FDB teamTracker-style): a monitored
  # assignment died. First make the gap keyspace-visible — publish the
  # placeholder ref under the fence, so clients park instead of
  # hammering the corpse and displaced-but-alive twins observe the
  # change — then tell the placeholder the tag is uncovered (park, don't
  # forward) and eagerly re-recruit. A superseded publish cedes: a newer
  # owner heals, not us.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = t) when is_map_key(t.assignment_monitors, ref) do
    {tag, monitors} = Map.pop(t.assignment_monitors, ref)
    t = %{t | assignment_monitors: monitors}

    Logger.warning("Bedrock distributor (epoch #{t.epoch}): materializer for tag #{tag} down (#{inspect(reason)})")
    Placeholder.notify_uncovered(t.placeholder, tag)

    case publish_placeholders(t, [tag]) do
      :ok ->
        refs = Map.put(t.snapshot.materializer_refs, tag, {@placeholder_worker_id, Atom.to_string(node())})
        {:noreply, maybe_recruit(%{t | snapshot: %{t.snapshot | materializer_refs: refs}}, tag)}

      {:error, :superseded} ->
        Logger.info("Bedrock distributor (epoch #{t.epoch}): superseded healing tag #{tag}; ceding")
        {:stop, :normal, t}

      {:error, reason2} ->
        # The gap stays; the corpse's clients fail :unavailable and the
        # next demand or DOWN-less poll tick has another chance once the
        # transient clears. Re-recruit anyway — publish retries on
        # completion.
        Logger.warning(
          "Bedrock distributor (epoch #{t.epoch}): healing publish for tag #{tag} failed: #{inspect(reason2)}"
        )

        {:noreply, maybe_recruit(t, tag)}
    end
  end

  # A crashed placeholder restarts under the SAME registered name on the
  # same node, so the committed placeholder refs stay valid — no
  # republication (the option-2 addressing dividend). Parked requests
  # died with it; their callers time out and retry, which re-parks.
  def handle_info({:EXIT, pid, reason}, %State{placeholder: pid} = t) do
    Logger.warning("Bedrock distributor (epoch #{t.epoch}): placeholder exited #{inspect(reason)}; restarting")

    case start_placeholder(t, t.snapshot.shard_layout) do
      {:ok, placeholder} -> {:noreply, %{t | placeholder: placeholder}}
      {:error, reason2} -> {:stop, {:shutdown, {:placeholder_restart_failed, reason2}}, t}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, %State{} = t), do: {:noreply, t}

  # An existing placeholder ref counts as covered-enough to skip
  # republication. That rests on two invariants: the distributor runs on
  # the director's node (a restarted placeholder re-registers the same
  # name on the node the committed refs already name), and every
  # recovery's bootstrap re-assigns all layout tags, so placeholder refs
  # never survive a recovery as stale foreign-node entries. If the
  # distributor ever detaches from the director's node, this rejection
  # must compare the ref's node too.
  defp uncovered_tags(%{shard_layout: shard_layout, materializer_refs: refs}) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {tag, _start_key} -> tag end)
    |> Enum.uniq()
    |> Enum.reject(&Map.has_key?(refs, &1))
  end

  defp publish_placeholders(_t, []), do: :ok

  defp publish_placeholders(%State{} = t, tags) do
    node_string = Atom.to_string(node())

    mutations =
      Enum.map(tags, fn tag ->
        {:set, SystemKeys.materializer_key(tag), Values.encode_materializer_ref(@placeholder_worker_id, node_string)}
      end)

    with :ok <- Transactions.commit_checked(t.lock, t.deps, mutations) do
      Telemetry.emit_placeholder_published(t.cluster, tags)
      :ok
    end
  end

  defp start_placeholder(%State{} = t, shard_layout) do
    start_fn = t.placeholder_start_fn || (&default_start_placeholder/1)

    start_fn.(
      cluster: t.cluster,
      distributor: self(),
      shard_layout: shard_layout,
      otp_name: t.cluster.otp_name_for_worker(@placeholder_worker_id)
    )
  end

  defp default_start_placeholder(opts) do
    %{start: {m, f, a}} = Placeholder.Server.child_spec(opts)
    apply(m, f, a)
  end

  defp maybe_recruit(%State{recruitment_ctx: nil} = t, _tag), do: t

  defp maybe_recruit(%State{} = t, tag) do
    cond do
      MapSet.member?(t.recruiting, tag) -> t
      in_backoff?(t, tag) -> t
      true -> start_recruitment(t, tag)
    end
  end

  # spawn_monitor, not a bare task: a crashed recruit task that never
  # sends its completion would otherwise leave the tag in the in-flight
  # set forever — unrecruitable for the rest of the epoch. The DOWN
  # handler converts an abnormal exit into a synthetic failed completion.
  defp start_recruitment(%State{} = t, tag) do
    server = self()
    ctx = t.recruitment_ctx

    {_pid, ref} =
      spawn_monitor(fn ->
        send(server, {:recruitment_complete, tag, Recruitment.recruit(tag, ctx)})
      end)

    %{t | recruiting: MapSet.put(t.recruiting, tag), recruit_task_refs: Map.put(t.recruit_task_refs, ref, tag)}
  end

  defp publish_recruit(%State{} = t, tag, pid, node, worker_id) do
    node_string = Atom.to_string(node)
    mutation = {:set, SystemKeys.materializer_key(tag), Values.encode_materializer_ref(worker_id, node_string)}

    case Transactions.commit_checked(t.lock, t.deps, [mutation]) do
      :ok ->
        callable = {t.cluster.otp_name_for_worker(worker_id), node}
        Placeholder.notify_covered(t.placeholder, tag, callable)

        refs = Map.put(t.snapshot.materializer_refs, tag, {worker_id, node_string})

        {:noreply,
         monitor_assignment(
           %{
             t
             | snapshot: %{t.snapshot | materializer_refs: refs},
               pending_demands: MapSet.delete(t.pending_demands, tag)
           },
           tag,
           callable
         )}

      {:error, :superseded} ->
        # The READ verdict refused before any commit was attempted: this
        # recruit was definitively never fenced into the family — remove
        # the orphan and cede.
        Recruitment.remove_orphaned_worker(worker_id, node, t.recruitment_ctx)
        Logger.info("Bedrock distributor (epoch #{t.epoch}): superseded publishing tag #{tag}; ceding")
        {:stop, :normal, t}

      {:error, reason} ->
        # Removal is destruction and demands an unambiguous verdict. An
        # exhausted ABORT and a failed READ are definitive (nothing
        # committed): the worker is a true orphan. A commit TIMEOUT is
        # not — the commit may have landed, and removing the worker would
        # durably name a deleted worker in the keyspace: an unhealable
        # black hole until the next recovery. Leave the ambiguous case
        # running; its params carry the shard assignment, so healing and
        # the next recovery's re-adoption can reconcile it either way.
        if commit_definitely_not_landed?(reason) do
          Recruitment.remove_orphaned_worker(worker_id, node, t.recruitment_ctx)
        end

        Placeholder.notify_coverage_failed(t.placeholder, tag, reason)
        _ = pid
        {:noreply, start_backoff(t, tag)}
    end
  end

  defp commit_definitely_not_landed?({:lock_commit_failed, :aborted}), do: true
  defp commit_definitely_not_landed?({:lock_read_failed, _}), do: true
  defp commit_definitely_not_landed?({:read_version_failed, _}), do: true
  defp commit_definitely_not_landed?(_ambiguous), do: false

  # Monitor every live (non-placeholder) assignment by its callable name
  # — monitors on {name, node} fire on death OR unreachability, either of
  # which is a coverage gap worth healing.
  defp monitor_assignments(%State{} = t) do
    Enum.reduce(t.snapshot.materializer_refs, t, fn
      {_tag, {@placeholder_worker_id, _node}}, acc ->
        acc

      {tag, {worker_id, node}}, acc ->
        # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
        monitor_assignment(acc, tag, {acc.cluster.otp_name_for_worker(worker_id), String.to_atom(node)})
    end)
  end

  defp monitor_assignment(%State{} = t, tag, {name, node_atom}) do
    # A local name is monitored as a bare atom: the {name, node} form
    # requires live distribution, which a single-node deployment
    # legitimately runs without.
    target = if node_atom == node(), do: name, else: {name, node_atom}
    ref = Process.monitor(target)
    %{t | assignment_monitors: Map.put(t.assignment_monitors, ref, tag)}
  end

  defp start_backoff(%State{} = t, tag),
    do: %{t | backoff: Map.put(t.backoff, tag, System.monotonic_time(:millisecond) + t.backoff_ms)}

  defp in_backoff?(%State{} = t, tag) do
    case Map.get(t.backoff, tag) do
      nil -> false
      until -> System.monotonic_time(:millisecond) < until
    end
  end

  defp callable_ref(cluster, worker_id, node) do
    # The documented exception to no-atoms-on-decode: system-mode-gated
    # writers, count bounded by cluster membership.
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    {cluster.otp_name_for_worker(worker_id), String.to_atom(node)}
  end

  defp schedule_poll(%State{} = t) do
    Process.send_after(self(), :poll_lock, t.poll_interval_ms)
    t
  end
end
