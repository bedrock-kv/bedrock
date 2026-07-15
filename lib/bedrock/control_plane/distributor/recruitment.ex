defmodule Bedrock.ControlPlane.Distributor.Recruitment do
  @moduledoc """
  Recruits a materializer for an uncovered shard on behalf of the
  Distributor.

  Mirrors the recovery-time machinery in `MaterializerBootstrapPhase` so
  materializer lifecycles pass through a single set of conventions: pick a
  materializer-capable node from the coordinator-supplied capabilities, ask
  that node's Foreman to create a worker, lock it for recovery at the
  current epoch, and unlock it with the durable version the worker itself
  reported at lock time (zero for a freshly created worker, so it replays
  the shard's full history) and a transaction system layout filtered to
  the shard's logs so it starts pulling immediately.

  The Foreman/Materializer calls are injectable through the context map
  (`:create_worker_fn`, `:lock_materializer_fn`, `:unlock_materializer_fn`,
  `:remove_worker_fn`)
  using the same seam conventions as the bootstrap phase, so tests can stub
  the worker layer without reimplementing the plumbing.
  """

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  require Logger

  @type create_worker_fn ::
          (foreman :: {atom(), node()}, Worker.id(), :materializer, keyword() ->
             {:ok, Worker.ref()} | {:error, term()})
  @type lock_materializer_fn ::
          (worker :: {Worker.ref(), node()}, Bedrock.epoch() ->
             {:ok, pid(), Materializer.recovery_info()} | {:error, term()})
  @type unlock_materializer_fn ::
          (pid(), Bedrock.version(), TransactionSystemLayout.t() ->
             :ok | {:error, term()} | {:failure, term(), term()})
  @type remove_worker_fn ::
          (foreman :: {atom(), node()}, Worker.id(), keyword() -> :ok | {:error, term()})
  @type info_fn ::
          (worker :: {Worker.ref(), node()}, [Materializer.fact_name()], keyword() ->
             {:ok, %{Materializer.fact_name() => term()}} | {:error, term()})

  @type context :: %{
          required(:cluster) => module(),
          required(:epoch) => Bedrock.epoch(),
          required(:durable_version) => Bedrock.version(),
          required(:transaction_system_layout) => TransactionSystemLayout.t() | %{},
          required(:node_capabilities) => %{Bedrock.Cluster.capability() => [node()]},
          optional(:create_worker_fn) => create_worker_fn(),
          optional(:lock_materializer_fn) => lock_materializer_fn(),
          optional(:unlock_materializer_fn) => unlock_materializer_fn(),
          optional(:remove_worker_fn) => remove_worker_fn(),
          optional(:info_fn) => info_fn(),
          optional(:readoption_deadline_ms) => pos_integer()
        }

  # A candidate must answer its identity query well inside the point where
  # anyone could be waiting on the answer; placeholder coverage is already
  # live, so this is a pure-upgrade budget, not a liveness one.
  @readoption_deadline_ms 2_000

  @doc """
  Recruits a materializer for the given shard tag: node selection, worker
  creation via the node's Foreman, epoch lock, and unlock with the shard's
  logs. Returns the live materializer pid, the node it was placed on, and
  the worker id it was created under (so the caller can remove the worker
  if fencing the recruit into the layout fails).

  A worker that was created but never reached service (lock or unlock
  failed) is removed again via `remove_orphaned_worker/3` before the error
  is returned, so failed recruitment does not leak idle or half-locked
  workers on the node.
  """
  @spec recruit(Bedrock.range_tag(), context()) ::
          {:ok, pid(), node(), Worker.id()} | {:error, term()}
  def recruit(tag, context) do
    with {:ok, node} <- find_materializer_capable_node(context.node_capabilities),
         {:ok, worker_ref, worker_id} <- create_materializer_worker(node, tag, context) do
      with {:ok, pid, recovery_info} <- lock_materializer({worker_ref, node}, node, context),
           :ok <- unlock_and_start_pulling(pid, tag, node, recovery_info, context) do
        {:ok, pid, node, worker_id}
      else
        {:error, _reason} = error ->
          remove_orphaned_worker(worker_id, node, context)
          error
      end
    end
  end

  @doc """
  Re-adopts an existing materializer worker into coverage for the given
  shard tag: recruitment minus worker creation. The worker (typically a
  previous epoch's materializer the services map still names) is epoch
  locked and unlocked with the durable version IT reports at lock time, so
  it resumes pulling from exactly where its own store left off.

  Unlike a failed recruitment, a failed re-adoption never removes the
  worker: it pre-exists this attempt and holds real state. The caller
  leaves the tag on placeholder coverage and demand-driven recruitment
  remains the fallback.
  """
  @spec readopt(Bedrock.range_tag(), worker :: {Worker.ref(), node()}, context()) ::
          {:ok, pid(), node()} | {:error, term()}
  def readopt(tag, {_worker_ref, node} = worker, context) do
    with {:ok, pid, recovery_info} <- lock_materializer(worker, node, context),
         :ok <- unlock_and_start_pulling(pid, tag, node, recovery_info, context) do
      {:ok, pid, node}
    end
  end

  @doc """
  Identifies re-adoption candidates among the given services for the given
  uncovered shard tags: every `:materializer` service is asked for its
  `:shard_id` fact concurrently, each under a short deadline, and the first
  candidate claiming each uncovered tag wins its slot. Candidates that are
  slow, dead, unreachable, or claim no/other shards are simply dropped -
  placeholder coverage is already live, so identification is a pure
  best-effort upgrade and must never block on a corpse.
  """
  @spec identify_readoption_candidates(
          services :: %{Worker.id() => {atom(), {Worker.ref(), node()}}},
          tags :: MapSet.t(Bedrock.range_tag()),
          context()
        ) :: %{Bedrock.range_tag() => {Worker.ref(), node()}}
  def identify_readoption_candidates(services, tags, context) do
    info_fn = Map.get(context, :info_fn, &Materializer.info/3)
    deadline_ms = Map.get(context, :readoption_deadline_ms, @readoption_deadline_ms)

    candidates = for {_id, {:materializer, {name, node}}} <- services, do: {name, node}

    candidates
    |> Task.async_stream(
      fn worker -> {worker, candidate_shard_id(info_fn, worker, deadline_ms)} end,
      max_concurrency: max(length(candidates), 1),
      ordered: false,
      timeout: deadline_ms,
      on_timeout: :kill_task
    )
    |> Enum.reduce(%{}, fn
      {:ok, {worker, {:ok, shard_id}}}, acc ->
        if MapSet.member?(tags, shard_id) and not Map.has_key?(acc, shard_id) do
          Map.put(acc, shard_id, worker)
        else
          acc
        end

      _slow_dead_or_unidentified, acc ->
        acc
    end)
  end

  # One short-timeout info call per candidate; a dead node or unregistered
  # name exits the call, which counts the candidate out rather than raising.
  defp candidate_shard_id(info_fn, worker, deadline_ms) do
    case info_fn.(worker, [:shard_id], timeout_in_ms: deadline_ms) do
      {:ok, %{shard_id: shard_id}} when is_integer(shard_id) -> {:ok, shard_id}
      _no_identity -> :error
    end
  catch
    _kind, _reason -> :error
  end

  @doc """
  Best-effort removal of a worker left behind by a failed recruitment. The
  worker never carried data a client could reach, so removal is safe; any
  failure to remove it (foreman unreachable, worker already gone) is
  logged and swallowed - orphan cleanup must never mask the original
  recruitment error.
  """
  @spec remove_orphaned_worker(Worker.id(), node(), context()) :: :ok
  def remove_orphaned_worker(worker_id, node, context) do
    remove_worker_fn = Map.get(context, :remove_worker_fn, &Foreman.remove_worker/3)
    foreman_ref = {context.cluster.otp_name(:foreman), node}

    try do
      remove_worker_fn.(foreman_ref, worker_id, timeout: 5_000)
    catch
      kind, reason ->
        Logger.warning(
          "Failed to remove orphaned materializer worker #{inspect(worker_id)} " <>
            "on #{inspect(node)}: #{inspect({kind, reason})}"
        )
    end

    :ok
  end

  # Placement: same convention as the recovery bootstrap phase - the first
  # materializer-capable node from the coordinator's capability directory.
  defp find_materializer_capable_node(node_capabilities) do
    case Map.get(node_capabilities, :materializer, []) do
      [node | _] -> {:ok, node}
      [] -> {:error, :no_materializer_capable_nodes}
    end
  end

  defp create_materializer_worker(node, tag, context) do
    foreman_ref = {context.cluster.otp_name(:foreman), node}
    worker_id = Worker.random_id()
    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)

    # The shard assignment is recorded in the worker's manifest params so
    # the worker can later be identified (`:shard_id` info fact) and
    # re-adopted after an epoch change.
    case create_worker_fn.(foreman_ref, worker_id, :materializer, timeout: 30_000, params: %{"shard_id" => tag}) do
      {:ok, worker_ref} -> {:ok, worker_ref, worker_id}
      {:error, reason} -> {:error, {:worker_creation_failed, reason, tag, node}}
    end
  end

  defp lock_materializer(worker, node, context) do
    lock_fn = Map.get(context, :lock_materializer_fn, &default_lock_materializer/2)

    case lock_fn.(worker, context.epoch) do
      {:ok, pid, recovery_info} -> {:ok, pid, recovery_info}
      {:error, reason} -> {:error, {:materializer_lock_failed, reason, node}}
    end
  end

  defp default_lock_materializer(worker, epoch), do: Materializer.lock_for_recovery(worker, epoch)

  defp unlock_and_start_pulling(pid, tag, node, recovery_info, context) do
    tsl = shard_tsl(tag, context)
    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(pid, start_version(recovery_info, context), tsl) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unlock_failed, reason, node}}
      {:failure, reason, _ref} -> {:error, {:unlock_failed, reason, node}}
    end
  end

  # The unlock durable_version tells the materializer which version its own
  # store already reflects - it starts pulling from the logs *after* that
  # point. A recruited worker is freshly created (empty), so we must use the
  # version IT reports at lock time (zero for a new store): unlocking it at
  # the cluster-wide recovery durable version would silently skip every
  # transaction before recovery. This matches the bootstrap phase, which
  # unlocks newly created workers at version zero.
  defp start_version(recovery_info, context) when is_map(recovery_info),
    do: Map.get(recovery_info, :durable_version) || context.durable_version

  defp start_version(recovery_info, context) when is_list(recovery_info),
    do: Keyword.get(recovery_info, :durable_version) || context.durable_version

  defp start_version(_recovery_info, context), do: context.durable_version

  defp default_unlock_materializer(pid, durable_version, tsl),
    do: Materializer.unlock_after_recovery(pid, durable_version, tsl, timeout_in_ms: 30_000)

  # Builds the layout handed to the recruited materializer, filtered to the
  # logs that route to its shard (same shape the bootstrap phase builds).
  defp shard_tsl(tag, %{transaction_system_layout: snapshot, epoch: epoch}) do
    %{
      id: TransactionSystemLayout.random_id(),
      epoch: epoch,
      director: :unavailable,
      sequencer: Map.get(snapshot, :sequencer),
      rate_keeper: nil,
      proxies: Map.get(snapshot, :proxies, []),
      resolvers: Map.get(snapshot, :resolvers, []),
      logs: snapshot |> Map.get(:logs, %{}) |> filter_logs_for_shard(tag),
      services: Map.get(snapshot, :services, %{})
    }
  end

  defp filter_logs_for_shard(logs, tag) do
    logs
    |> Enum.filter(fn {_log_id, tags} -> log_routes_to_shard?(tags, tag) end)
    |> Map.new()
  end

  defp log_routes_to_shard?([], _tag), do: true
  defp log_routes_to_shard?(tags, tag), do: tag in tags
end
