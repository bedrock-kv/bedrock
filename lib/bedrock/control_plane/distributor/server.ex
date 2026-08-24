defmodule Bedrock.ControlPlane.Distributor.Server do
  @moduledoc false
  use GenServer

  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.ControlPlane.Distributor.Recruitment
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.ControlPlane.Distributor.Telemetry
  alias Bedrock.ControlPlane.Distributor.Transactions
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  require Logger

  @placeholder_worker_id Placeholder.worker_id()

  # Consecutive failed ping-verifications before unreachability is
  # treated as death. With the default reverify interval this is ~6s of
  # sustained unreachability — long enough to ride out a dist blip,
  # short enough that a lost node heals promptly.
  @max_unreachable_verifications 3

  # Deadline for the sweep's per-assignment epoch PROBE (the adopt that
  # may follow carries its own 30s lock/unlock bounds). Monitors and
  # placeholder coverage are already live, so the probe itself must
  # never wait on a wedged worker.
  @verification_timeout_ms 2_000

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
      backoff_ms: Keyword.get(opts, :backoff_ms, 5_000),
      reverify_interval_ms: Keyword.get(opts, :reverify_interval_ms, 2_000)
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
         uncovered = uncovered_tags(snapshot),
         {:ok, t} <- publish_placeholders(t, uncovered) do
      # Eager recruitment for the swept gaps erases first-touch latency:
      # the sweep already knows every uncovered tag, and the in-flight
      # dedupe and backoff machinery make eagerness free. Named
      # assignments get the complementary treatment: verification that
      # each is actually IN this epoch (see verify_assignments/1).
      t = Enum.reduce(uncovered, monitor_assignments(t), &maybe_recruit(&2, &1))
      {:noreply, verify_assignments(t)}
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

    case t |> coverage_members(tag) |> RoutingData.pick_member() do
      {:ok, {worker_id, node}} ->
        Placeholder.notify_covered(t.placeholder, tag, callable_ref(t.cluster, worker_id, node))
        {:noreply, t}

      :error ->
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
        publish_assignment(t, tag, pid, node, worker_id, :created)

      {:error, reason} ->
        Logger.warning("Bedrock distributor (epoch #{t.epoch}): recruitment for tag #{tag} failed: #{inspect(reason)}")
        Placeholder.notify_coverage_failed(t.placeholder, tag, reason)
        {:noreply, start_backoff(t, tag)}
    end
  end

  # Verification verdicts serialize back through the server. A verdict
  # for a worker the committed set no longer contains is DROPPED: some
  # other mechanism (death healing, idle retirement, a newer owner)
  # already removed it, and a late-adopted stray retires itself in-band.
  # Nothing is reserved while a probe is in flight — with set-valued
  # membership an extra materializer is legal, so the only cost of a
  # concurrent recruit is a redundant worker, never a lost heal.
  def handle_info({:assignment_verified, tag, worker_id, verdict}, %State{} = t) do
    cond do
      not Map.has_key?(real_members(t, tag), worker_id) ->
        {:noreply, clear_unreachable(t, tag, worker_id)}

      verdict == :current ->
        {:noreply, clear_unreachable(t, tag, worker_id)}

      match?({:adopted, _pid, _node, _worker_id}, verdict) ->
        {:adopted, pid, node, adopted_id} = verdict
        publish_assignment(clear_unreachable(t, tag, worker_id), tag, pid, node, adopted_id, :preexisting)

      match?({:error, reason} when reason in [:unavailable, :timeout], verdict) ->
        # Unreachable-shaped: the same evidence a :noconnection DOWN
        # carries, damped the same way — a dist blip at sweep time (the
        # post-recovery moment nodes are still rejoining) must not heal
        # every shard on the node at once. Escalates through the shared
        # counter; the reverify tick re-runs verification on contact.
        Logger.warning("Bedrock distributor (epoch #{t.epoch}): tag #{tag} unreachable during verification; damping")
        escalate_unreachable(t, tag, worker_id)

      true ->
        {:error, reason} = verdict

        Logger.warning(
          "Bedrock distributor (epoch #{t.epoch}): materializer #{worker_id} for tag #{tag} failed verification " <>
            "(#{inspect(reason)}); healing"
        )

        heal_member(clear_unreachable(t, tag, worker_id), tag, worker_id)
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

  # A verification task that died without reporting leaves nothing to
  # clean but its ref: the member keeps its entry, its monitor, and its
  # place in the set, and the next epoch's sweep verifies it again.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = t) when is_map_key(t.verification_task_refs, ref),
    do: {:noreply, %{t | verification_task_refs: Map.delete(t.verification_task_refs, ref)}}

  # A retirement whose commit failed transiently: retry until the
  # keyspace stops naming a worker that is gone.
  # ...unless the member was legitimately re-published in the meantime
  # (an adoption verdict for a worker that turned out to be alive and
  # this epoch's). "Still a member" alone cannot tell that apart from
  # "never retired", so the pending entry is the token: publishing
  # clears it, and a retry without one is stale.
  def handle_info({:retire_member, tag, worker_id}, %State{} = t) do
    if Map.has_key?(real_members(t, tag), worker_id) and Map.has_key?(t.pending_retires, {tag, worker_id}),
      do: retire_member(%{t | pending_retires: Map.delete(t.pending_retires, {tag, worker_id})}, tag, worker_id),
      else: {:noreply, %{t | pending_retires: Map.delete(t.pending_retires, {tag, worker_id})}}
  end

  # Death healing (event-driven, FDB teamTracker-style) — with FDB's
  # distinction between connection state and failure: unreachable is NOT
  # dead. A :noconnection DOWN fires for every remote assignment at once
  # on any transient dist blip; healing eagerly on it would rip live
  # workers out of the routing view and stampede recruitment (N blips →
  # N+1 workers per shard). Instead: damp — ping-verify on a timer, heal
  # only after the node stays unreachable across consecutive checks.
  # Genuine death signals (:noproc, :killed, crashes) heal immediately.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = t) when is_map_key(t.assignment_monitors, ref) do
    {{tag, worker_id}, monitors} = Map.pop(t.assignment_monitors, ref)
    t = %{t | assignment_monitors: monitors}

    case reason do
      :noconnection ->
        Logger.warning("Bedrock distributor (epoch #{t.epoch}): materializer #{worker_id} unreachable; verifying")
        {:noreply, schedule_reverify(t, tag, worker_id)}

      {:shutdown, :idle} ->
        # A voluntary idle spin-down (bedrock-q67.21.5): the shard proved
        # cold, so revival is demand-driven — retire the member but do
        # NOT eagerly re-recruit; the next read parks and re-demands.
        Logger.info("Bedrock distributor (epoch #{t.epoch}): #{worker_id} spun down idle; revival on demand")
        Telemetry.emit_idle_spindown(t.cluster, tag)
        retire_member(clear_unreachable(t, tag, worker_id), tag, worker_id)

      _dead ->
        Logger.warning("Bedrock distributor (epoch #{t.epoch}): materializer #{worker_id} down (#{inspect(reason)})")
        heal_member(clear_unreachable(t, tag, worker_id), tag, worker_id)
    end
  end

  # The reverify tick: reachable again means the worker was never
  # observed dead — reset the count and re-arm the monitor (a genuinely
  # dead worker on a reachable node yields an immediate :noproc DOWN and
  # heals through the fast path above). Persistent unreachability
  # escalates to a heal after @max_unreachable_verifications consecutive
  # failed pings. A tag whose ref changed meanwhile has nothing left to
  # verify.
  def handle_info({:reverify_assignment, tag, worker_id}, %State{} = t) do
    case t |> real_members(tag) |> Map.fetch(worker_id) do
      {:ok, node_string} -> reverify_assignment(t, tag, worker_id, node_string)
      :error -> {:noreply, clear_unreachable(t, tag, worker_id)}
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

  # Uncovered means the tag's committed member set holds no REAL worker.
  # The placeholder is an ordinary member of that set — it parks reads
  # rather than serving them — so its presence is not coverage, and
  # adding it never displaces a live worker.
  defp uncovered_tags(%{shard_layout: shard_layout, materializer_refs: refs}) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {tag, _start_key} -> tag end)
    |> Enum.uniq()
    |> Enum.filter(fn tag -> refs |> Map.get(tag, %{}) |> Map.delete(@placeholder_worker_id) == %{} end)
  end

  defp publish_placeholders(%State{} = t, []), do: {:ok, t}

  defp publish_placeholders(%State{} = t, tags) do
    # A tag already carrying OUR placeholder needs no write: same name,
    # same node, so re-setting it would be a commit that changes
    # nothing. A placeholder naming a different node is a prior epoch's
    # leak — it parks nothing we can reach, so it must be overwritten,
    # not trusted.
    case Enum.reject(tags, &placeholder_here?(t, &1)) do
      [] ->
        {:ok, t}

      missing ->
        with :ok <- Transactions.commit_checked(t.lock, t.deps, Enum.map(missing, &placeholder_mutation/1)) do
          Telemetry.emit_placeholder_published(t.cluster, missing)
          # Record what we just committed: every later placeholder
          # decision reads this view, and a view that omits a committed
          # placeholder omits its clear when a real member lands.
          {:ok, Enum.reduce(missing, t, &put_member(&2, &1, @placeholder_worker_id, our_node_string()))}
        end
    end
  end

  # Coverage the placeholder can actually provide: the entry must name
  # this distributor's own node, because a parked read is held by the
  # local placeholder process. A placeholder key naming a dead node
  # routes clients at nothing.
  defp placeholder_here?(%State{} = t, tag), do: Map.get(members(t, tag), @placeholder_worker_id) == our_node_string()

  defp our_node_string, do: Atom.to_string(node())

  defp placeholder_mutation(tag) do
    {:set, SystemKeys.materializer_key(tag, @placeholder_worker_id), Values.encode_materializer_node(our_node_string())}
  end

  defp members(%State{} = t, tag), do: Map.get(t.snapshot.materializer_refs, tag, %{})

  defp real_members(%State{} = t, tag), do: t |> members(tag) |> Map.delete(@placeholder_worker_id)

  # Members that can actually serve a read: a member whose retirement is
  # awaiting a retry is still committed (the keyspace names it, honestly)
  # but it is known-gone, so draining parked reads into it would undo the
  # park for nothing.
  defp coverage_members(%State{} = t, tag) do
    t
    |> real_members(tag)
    |> Map.reject(fn {worker_id, _node} -> Map.has_key?(t.pending_retires, {tag, worker_id}) end)
  end

  defp put_member(%State{} = t, tag, worker_id, node_string) do
    refs =
      Map.update(t.snapshot.materializer_refs, tag, %{worker_id => node_string}, &Map.put(&1, worker_id, node_string))

    %{t | snapshot: %{t.snapshot | materializer_refs: refs}}
  end

  defp drop_member(%State{} = t, tag, worker_id) do
    refs =
      case t |> members(tag) |> Map.delete(worker_id) do
        empty when empty == %{} -> Map.delete(t.snapshot.materializer_refs, tag)
        remaining -> Map.put(t.snapshot.materializer_refs, tag, remaining)
      end

    %{t | snapshot: %{t.snapshot | materializer_refs: refs}}
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

  # Publishes an assignment under the CHECK fence. Provenance decides
  # the failure policy: a :created recruit that was never fenced in is
  # an orphan this distributor made and may remove (verdicts permitting)
  # — a :preexisting adopted worker is NEVER removed (it holds real
  # state and its entry already names it; only the fence confirmation
  # was lost).
  defp publish_assignment(%State{} = t, tag, pid, node, worker_id, provenance) do
    node_string = Atom.to_string(node)
    callable = {t.cluster.otp_name_for_worker(worker_id), node}

    # Adding a real member retires the tag's placeholder in the same
    # fenced commit: parking exists only while nothing serves, and one
    # transaction means no window where both or neither is true.
    mutations =
      [{:set, SystemKeys.materializer_key(tag, worker_id), Values.encode_materializer_node(node_string)}] ++
        placeholder_retirement(t, tag)

    case Transactions.commit_checked(t.lock, t.deps, mutations) do
      :ok ->
        Placeholder.notify_covered(t.placeholder, tag, callable)

        {:noreply,
         t
         |> put_member(tag, worker_id, node_string)
         |> drop_member(tag, @placeholder_worker_id)
         # This member is legitimately in the set again; any retirement
         # still awaiting a retry is now stale and must not fire.
         |> cancel_retire(tag, worker_id)
         |> Map.update!(:pending_demands, &MapSet.delete(&1, tag))
         |> monitor_assignment(tag, worker_id, callable)}

      {:error, :superseded} ->
        # The READ verdict refused before any commit was attempted: a
        # :created recruit was definitively never fenced into the family
        # — remove the orphan. Either way, cede.
        if provenance == :created, do: Recruitment.remove_orphaned_worker(worker_id, node, t.recruitment_ctx)
        Logger.info("Bedrock distributor (epoch #{t.epoch}): superseded publishing tag #{tag}; ceding")
        {:stop, :normal, t}

      {:error, reason} when provenance == :created ->
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

      {:error, reason} ->
        # :preexisting — the entry already names this worker and it is
        # now serving; only the fence re-assertion was lost transiently.
        # Monitor it and let the poll loop deliver any supersession.
        Logger.warning(
          "Bedrock distributor (epoch #{t.epoch}): adoption re-assert for tag #{tag} failed: #{inspect(reason)}"
        )

        {:noreply, monitor_assignment(t, tag, worker_id, callable)}
    end
  end

  defp placeholder_retirement(%State{} = t, tag) do
    if Map.has_key?(members(t, tag), @placeholder_worker_id),
      do: [{:clear, SystemKeys.materializer_key(tag, @placeholder_worker_id)}],
      else: []
  end

  defp commit_definitely_not_landed?({:lock_commit_failed, :aborted}), do: true
  defp commit_definitely_not_landed?({:lock_read_failed, _}), do: true
  defp commit_definitely_not_landed?({:read_version_failed, _}), do: true
  defp commit_definitely_not_landed?(_ambiguous), do: false

  defp reverify_assignment(%State{} = t, tag, worker_id, node_string) do
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    node_atom = String.to_atom(node_string)

    if node_atom == node() or Node.ping(node_atom) == :pong do
      # Contact restored: re-arm the monitor (a genuinely dead worker on
      # a reachable node yields an immediate :noproc and heals through
      # the fast path; one that idle-spun-down during the blip is healed
      # the same way, its {:shutdown, :idle} intent lost — safe, mildly
      # wasteful, not worth persisting intent to avoid). Reachability is
      # not membership, though: verification re-runs, and ITS verdict —
      # not the ping — clears the escalation counter, so a wedged-alive
      # worker whose node pings fine still escalates to a heal.
      t = monitor_assignment(t, tag, worker_id, {t.cluster.otp_name_for_worker(worker_id), node_atom})

      t =
        if t.recruitment_ctx,
          do: start_verification(t, tag, worker_id, node_string),
          else: clear_unreachable(t, tag, worker_id)

      {:noreply, t}
    else
      escalate_unreachable(t, tag, worker_id)
    end
  end

  # The shared unreachability escalation: fed by failed pings AND
  # unreachable-shaped verification verdicts. Heals only after
  # @max_unreachable_verifications consecutive pieces of evidence.
  defp escalate_unreachable(%State{} = t, tag, worker_id) do
    count = Map.get(t.unreachable_counts, {tag, worker_id}, 0) + 1
    t = %{t | unreachable_counts: Map.put(t.unreachable_counts, {tag, worker_id}, count)}

    if count >= @max_unreachable_verifications do
      Logger.warning(
        "Bedrock distributor (epoch #{t.epoch}): #{worker_id} unreachable after #{count} verifications; healing"
      )

      heal_member(clear_unreachable(t, tag, worker_id), tag, worker_id)
    else
      {:noreply, schedule_reverify(t, tag, worker_id)}
    end
  end

  # A heal is retirement plus an eager replacement; an idle spin-down is
  # retirement alone (the shard proved cold — the next read revives it).
  # Healing is retire-then-recruit, guarded on membership: a signal for
  # a member the committed set no longer names is STALE (some other
  # mechanism already retired it), and healing it would recruit a
  # replica nothing asked for — unboundedly, since every redundant
  # member is itself monitored.
  defp heal_member(%State{} = t, tag, worker_id) do
    if Map.has_key?(real_members(t, tag), worker_id) do
      case retire_member(t, tag, worker_id) do
        {:noreply, t2} -> {:noreply, maybe_recruit(t2, tag)}
        {:stop, _reason, _t} = stop -> stop
      end
    else
      {:noreply, t}
    end
  end

  # Retirement is a CLEAR of the departing member's own key — the family
  # names members individually, so removing one never touches another
  # and never has to overwrite a live twin's entry. When the clear would
  # leave the tag with no real member, the same fenced commit adds the
  # placeholder, so a shard is never briefly unroutable between the two.
  # A superseded commit cedes. A transient failure keeps the local view
  # honest (the keyspace still names the departed worker) and schedules
  # a retry: nothing else would revisit this tag, and an uncleared
  # corpse can be handed to clients by the deterministic pick. Until
  # that retry resolves, the member is listed in `pending_retires` — it
  # is still committed, but it is not coverage, so the demand path skips
  # it and a legitimate re-publication cancels the retry.
  defp retire_member(%State{} = t, tag, worker_id) do
    remaining = t |> real_members(tag) |> Map.delete(worker_id)
    parking? = remaining == %{}

    if parking?,
      do: Placeholder.notify_uncovered(t.placeholder, tag),
      else: notify_covered_pick(t, tag, remaining)

    mutations =
      [{:clear, SystemKeys.materializer_key(tag, worker_id)}] ++
        if parking? and not placeholder_here?(t, tag),
          do: [placeholder_mutation(tag)],
          else: []

    case Transactions.commit_checked(t.lock, t.deps, mutations) do
      :ok ->
        t = t |> drop_member(tag, worker_id) |> demonitor_member(tag, worker_id) |> cancel_retire(tag, worker_id)
        t = if parking?, do: put_member(t, tag, @placeholder_worker_id, our_node_string()), else: t
        {:noreply, t}

      {:error, :superseded} ->
        Logger.info("Bedrock distributor (epoch #{t.epoch}): superseded retiring #{worker_id} for tag #{tag}; ceding")
        {:stop, :normal, t}

      {:error, reason} ->
        Logger.warning(
          "Bedrock distributor (epoch #{t.epoch}): retiring #{worker_id} for tag #{tag} failed: " <>
            "#{inspect(reason)}; retrying"
        )

        timer = Process.send_after(self(), {:retire_member, tag, worker_id}, t.backoff_ms)
        {:noreply, %{t | pending_retires: Map.put(t.pending_retires, {tag, worker_id}, timer)}}
    end
  end

  # A partial retirement leaves the shard covered, but the placeholder
  # may still be forwarding to the member that just left (an earlier
  # notify_covered named it). Point it at the survivor the client-facing
  # pick would choose.
  defp notify_covered_pick(%State{} = t, tag, remaining) do
    case RoutingData.pick_member(remaining) do
      {:ok, {worker_id, node_string}} ->
        Placeholder.notify_covered(t.placeholder, tag, callable_ref(t.cluster, worker_id, node_string))

      :error ->
        :ok
    end
  end

  # Retirement disarms the member's monitor: an armed monitor for a
  # member no longer in the set turns that member's eventual death into
  # a heal, and a heal into a redundant replica.
  defp demonitor_member(%State{} = t, tag, worker_id) do
    case Enum.find(t.assignment_monitors, fn {_ref, member} -> member == {tag, worker_id} end) do
      {ref, _member} ->
        Process.demonitor(ref, [:flush])
        %{t | assignment_monitors: Map.delete(t.assignment_monitors, ref)}

      nil ->
        t
    end
  end

  defp cancel_retire(%State{} = t, tag, worker_id) do
    case Map.pop(t.pending_retires, {tag, worker_id}) do
      {nil, _} -> t
      {timer, rest} -> cancel_timer(t, timer, rest)
    end
  end

  defp cancel_timer(%State{} = t, timer, rest) do
    Process.cancel_timer(timer)
    %{t | pending_retires: rest}
  end

  defp clear_unreachable(%State{} = t, tag, worker_id),
    do: %{t | unreachable_counts: Map.delete(t.unreachable_counts, {tag, worker_id})}

  defp schedule_reverify(%State{} = t, tag, worker_id) do
    Process.send_after(self(), {:reverify_assignment, tag, worker_id}, t.reverify_interval_ms)
    t
  end

  # The membership check the committed family demands (FDB's DD verifies
  # every serverList entry the same way; membership is never assumed
  # from liveness): each named assignment is asked, bounded, which epoch
  # it was last locked into. Current answers verify silently. A stale or
  # never-locked answer means the epoch never embraced this worker — its
  # node missed recovery's roll call — and it is ADOPTED: locked at the
  # epoch, unlocked at its own durable version, its entry re-asserted
  # under the fence. Anything else (wedged, dead, unreachable beyond the
  # monitor's damping) is healed; the worker itself is never removed —
  # it retires in-band when it observes the replacing entry. One shot,
  # at the sweep. Nothing is reserved while a probe runs: membership is
  # a set, so a concurrent recruit costs a redundant worker, never a
  # lost heal.
  defp verify_assignments(%State{recruitment_ctx: nil} = t), do: t

  defp verify_assignments(%State{} = t) do
    Enum.reduce(each_real_member(t), t, fn {tag, worker_id, node_string}, acc ->
      start_verification(acc, tag, worker_id, node_string)
    end)
  end

  # Every committed member of every tag except the placeholders, which
  # this distributor owns directly and never verifies or monitors.
  defp each_real_member(%State{} = t) do
    for {tag, members} <- t.snapshot.materializer_refs,
        {worker_id, node_string} <- Map.delete(members, @placeholder_worker_id),
        do: {tag, worker_id, node_string}
  end

  defp start_verification(%State{} = t, tag, worker_id, node_string) do
    server = self()
    ctx = t.recruitment_ctx
    epoch = t.epoch
    name = t.cluster.otp_name_for_worker(worker_id)
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    node_atom = String.to_atom(node_string)
    info_fn = Map.get(ctx, :info_fn, &Materializer.info/3)

    {_pid, ref} =
      spawn_monitor(fn ->
        verdict =
          try do
            case info_fn.({name, node_atom}, [:epoch], timeout_in_ms: @verification_timeout_ms) do
              {:ok, %{epoch: ^epoch}} -> :current
              {:ok, %{epoch: _stale_or_nil}} -> Recruitment.adopt(tag, worker_id, node_atom, ctx)
              {:error, reason} -> {:error, reason}
            end
          catch
            kind, reason -> {:error, {kind, reason}}
          end

        verdict =
          case verdict do
            {:ok, pid, node, id} -> {:adopted, pid, node, id}
            other -> other
          end

        send(server, {:assignment_verified, tag, worker_id, verdict})
      end)

    # Verification reserves nothing: with set-valued membership a
    # concurrent recruit costs a redundant worker, never a lost heal.
    # The task is still monitored so a crash cannot leak silently.
    %{t | verification_task_refs: Map.put(t.verification_task_refs, ref, {tag, worker_id})}
  end

  # Monitor every live (non-placeholder) assignment by its callable name
  # — monitors on {name, node} fire on death OR unreachability, either of
  # which is a coverage gap worth healing.
  defp monitor_assignments(%State{} = t) do
    Enum.reduce(each_real_member(t), t, fn {tag, worker_id, node}, acc ->
      # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
      monitor_assignment(acc, tag, worker_id, {acc.cluster.otp_name_for_worker(worker_id), String.to_atom(node)})
    end)
  end

  defp monitor_assignment(%State{} = t, tag, worker_id, {name, node_atom}) do
    if {tag, worker_id} in Map.values(t.assignment_monitors) do
      # Already armed (the sweep monitors before verification adopts):
      # a second monitor would double every DOWN into a double heal.
      t
    else
      # A local name is monitored as a bare atom: the {name, node} form
      # requires live distribution, which a single-node deployment
      # legitimately runs without.
      target = if node_atom == node(), do: name, else: {name, node_atom}
      ref = Process.monitor(target)
      %{t | assignment_monitors: Map.put(t.assignment_monitors, ref, {tag, worker_id})}
    end
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
