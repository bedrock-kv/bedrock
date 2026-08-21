defmodule Bedrock.ControlPlane.Distributor.Server do
  @moduledoc false
  use GenServer

  alias Bedrock.ControlPlane.Distributor.Placeholder
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
      placeholder_start_fn: Keyword.get(opts, :placeholder_start_fn)
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
      {:noreply, t}
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
  # placeholder the covered ref so it drains; otherwise record the
  # pending demand — recruitment consumes it (bedrock-q67.21.4).
  @impl true
  def handle_cast({:coverage_demand, tag}, %State{} = t) do
    Telemetry.emit_coverage_demand(t.cluster, tag)

    case Map.fetch(t.snapshot.materializer_refs, tag) do
      {:ok, {worker_id, node}} when worker_id != @placeholder_worker_id ->
        Placeholder.notify_covered(t.placeholder, tag, callable_ref(t.cluster, worker_id, node))
        {:noreply, t}

      _uncovered_or_placeholder ->
        {:noreply, %{t | pending_demands: MapSet.put(t.pending_demands, tag)}}
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
