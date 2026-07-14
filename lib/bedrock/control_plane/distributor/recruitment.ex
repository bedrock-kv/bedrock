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
  (`:create_worker_fn`, `:lock_materializer_fn`, `:unlock_materializer_fn`)
  using the same seam conventions as the bootstrap phase, so tests can stub
  the worker layer without reimplementing the plumbing.
  """

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  @type create_worker_fn ::
          (foreman :: {atom(), node()}, Worker.id(), :materializer, keyword() ->
             {:ok, Worker.ref()} | {:error, term()})
  @type lock_materializer_fn ::
          (worker :: {Worker.ref(), node()}, Bedrock.epoch() ->
             {:ok, pid(), Materializer.recovery_info()} | {:error, term()})
  @type unlock_materializer_fn ::
          (pid(), Bedrock.version(), TransactionSystemLayout.t() ->
             :ok | {:error, term()} | {:failure, term(), term()})

  @type context :: %{
          required(:cluster) => module(),
          required(:epoch) => Bedrock.epoch(),
          required(:durable_version) => Bedrock.version(),
          required(:transaction_system_layout) => TransactionSystemLayout.t() | %{},
          required(:node_capabilities) => %{Bedrock.Cluster.capability() => [node()]},
          optional(:create_worker_fn) => create_worker_fn(),
          optional(:lock_materializer_fn) => lock_materializer_fn(),
          optional(:unlock_materializer_fn) => unlock_materializer_fn()
        }

  @doc """
  Recruits a materializer for the given shard tag: node selection, worker
  creation via the node's Foreman, epoch lock, and unlock with the shard's
  logs. Returns the live materializer pid and the node it was placed on.
  """
  @spec recruit(Bedrock.range_tag(), context()) :: {:ok, pid(), node()} | {:error, term()}
  def recruit(tag, context) do
    with {:ok, node} <- find_materializer_capable_node(context.node_capabilities),
         {:ok, worker_ref} <- create_materializer_worker(node, tag, context),
         {:ok, pid, recovery_info} <- lock_materializer({worker_ref, node}, node, context),
         :ok <- unlock_and_start_pulling(pid, tag, node, recovery_info, context) do
      {:ok, pid, node}
    end
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

    case create_worker_fn.(foreman_ref, worker_id, :materializer, timeout: 30_000) do
      {:ok, worker_ref} -> {:ok, worker_ref}
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
