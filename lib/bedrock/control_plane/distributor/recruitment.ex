defmodule Bedrock.ControlPlane.Distributor.Recruitment do
  @moduledoc """
  On-demand materializer recruitment for the distributor: node
  selection, worker creation via the node's Foreman, epoch lock, and
  unlock with the shard's typed pull sources — the same replica set the
  commit proxies route with, resolved through `ShardRouter`, exactly as
  recovery's bootstrap seeds its unlocks.

  A worker that was created but never reached service (lock or unlock
  failed) is removed again before the error returns, so failed
  recruitment does not leak idle or half-locked workers. Publication of
  the recruit into the `materializers/` family is the CALLER's job (a
  check-fenced commit); a recruit whose publication aborts is an orphan
  and is removed the same way — commit abort replaces phase-a's
  delta-rejection as the orphan-cleanup trigger.
  """

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  require Logger

  @system_shard_tag RecoveryAttempt.system_shard_id()

  @typedoc """
  The recruitment context. `logs` and `log_refs` are the epoch's log
  wiring, handed by the director at recruit time (the same runtime
  wiring recover_from hands proxies): pull sources derive from them
  through the single `ShardRouter` placement site.
  """
  @type context :: %{
          required(:cluster) => module(),
          required(:epoch) => Bedrock.epoch(),
          required(:node_capabilities) => %{Bedrock.Cluster.capability() => [node()]},
          required(:logs) => %{Log.id() => [Bedrock.range_tag()]},
          required(:log_refs) => %{Log.id() => Log.ref()},
          optional(:worker_params) => %{String.t() => term()},
          optional(:create_worker_fn) => fun(),
          optional(:lock_materializer_fn) => fun(),
          optional(:unlock_materializer_fn) => fun(),
          optional(:remove_worker_fn) => fun()
        }

  @doc """
  Recruits a materializer for the given shard tag. Returns the live pid,
  the node it was placed on, and the worker id it was created under (so
  the caller can remove the worker if fencing the recruit into the
  family fails).
  """
  @spec recruit(Bedrock.range_tag(), context()) ::
          {:ok, pid(), node(), Worker.id()} | {:error, term()}
  def recruit(tag, context) do
    with {:ok, sources} <- pull_sources_for_shard(tag, context),
         {:ok, node} <- find_materializer_capable_node(context.node_capabilities),
         {:ok, worker_ref, worker_id} <- create_materializer_worker(node, tag, context) do
      with {:ok, pid, recovery_info} <- lock_materializer({worker_ref, node}, node, context),
           :ok <- unlock_and_start_pulling(pid, node, recovery_info, sources, context) do
        {:ok, pid, node, worker_id}
      else
        {:error, _reason} = error ->
          remove_orphaned_worker(worker_id, node, context)
          error
      end
    end
  end

  @doc """
  Adopts a family-named materializer into the current epoch: recruitment
  minus creation. The worker already exists (the committed
  `materializers/` family names it — the keyspace is the membership
  authority) but was never locked into this epoch, typically because its
  node missed recovery's roll call and rejoined later. It is locked at
  the epoch and unlocked at the durable version IT reports, so it
  resumes pulling from exactly where its own store left off.

  Unlike a failed recruitment, a failed adoption never removes the
  worker: it pre-exists this attempt and holds real state — enforced
  structurally by there being no removal call on this path. The caller
  heals the tag instead, and healing CLEARS this worker's own key — a
  clear the proxy privatizes onto the shard's stream, so the worker
  retires in-band, at the version its assignment ends
  (bedrock-q67.21.6).
  """
  @spec adopt(Bedrock.range_tag(), Worker.id(), node(), context()) ::
          {:ok, pid(), node(), Worker.id()} | {:error, term()}
  def adopt(tag, worker_id, node, context) do
    with {:ok, sources} <- pull_sources_for_shard(tag, context),
         {:ok, pid, recovery_info} <-
           lock_materializer({context.cluster.otp_name_for_worker(worker_id), node}, node, context),
         :ok <- unlock_and_start_pulling(pid, node, recovery_info, sources, context) do
      {:ok, pid, node, worker_id}
    end
  end

  @doc """
  Best-effort removal of a worker left behind by a failed recruitment or
  an aborted publication. The worker never carried data a client could
  reach, so removal is safe; any failure to remove it is logged and
  swallowed — orphan cleanup must never mask the original error.
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

  # Placement: same convention as the recovery bootstrap phase — the
  # first materializer-capable node from the capability directory.
  defp find_materializer_capable_node(node_capabilities) do
    case Map.get(node_capabilities, :materializer, []) do
      [node | _] -> {:ok, node}
      [] -> {:error, :no_materializer_capable_nodes}
    end
  end

  # Worker params (persisted in the manifest) always record the shard
  # assignment so the worker can be identified (:shard_id info fact) and
  # re-adopted after an epoch change, alongside any per-worker policy
  # params (e.g. "idle_timeout", bedrock-q67.21.5).
  defp create_materializer_worker(node, tag, context) do
    foreman_ref = {context.cluster.otp_name(:foreman), node}
    worker_id = Worker.random_id()
    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)

    case create_worker_fn.(foreman_ref, worker_id, :materializer,
           timeout: 30_000,
           params: worker_params(tag, context)
         ) do
      {:ok, worker_ref} -> {:ok, worker_ref, worker_id}
      {:error, reason} -> {:error, {:worker_creation_failed, reason, tag, node}}
    end
  end

  # The system shard takes the shard assignment and nothing else.
  # Recovery already seats tag 0 that way, but recovery is not the only
  # thing that creates a tag-0 materializer: the tag is in the shard
  # layout and in the `materializers/` family like any other, so it is
  # monitored, verified, and healed here (`Distributor.Server`'s
  # heal_member/3, and the startup sweep's uncovered set). A system
  # materializer created with the cluster's idle timeout would, after
  # its window of no CLIENT reads, upload a snapshot, delete its own
  # foreman entry and working directory, and exit — and the next
  # recovery stalls on a named system member it cannot reach
  # (MaterializerBootstrapPhase's resolve_system_materializer/2, whose
  # policy is deliberately stall-not-fallback). Tag 0 genuinely reaches
  # this site, so the exemption is stated here rather than assumed.
  @spec worker_params(Bedrock.range_tag(), context()) :: %{String.t() => term()}
  defp worker_params(tag, _context) when tag == @system_shard_tag, do: %{"shard_id" => tag}

  defp worker_params(tag, context), do: context |> Map.get(:worker_params, %{}) |> Map.put("shard_id", tag)

  defp lock_materializer(worker, node, context) do
    lock_fn = Map.get(context, :lock_materializer_fn, &default_lock_materializer/2)

    case lock_fn.(worker, context.epoch) do
      {:ok, pid, recovery_info} -> {:ok, pid, recovery_info}
      {:error, reason} -> {:error, {:materializer_lock_failed, reason, node}}
    end
  end

  # Bounded, like every other call in the pipeline: a created-but-wedged
  # worker (stuck mid snapshot download) must surface as a failed
  # recruitment the caller can shed and back off from, not wedge the
  # recruit task forever. (Phase-a documented exactly this hazard for
  # its unbounded lock and bounded it the same way.)
  defp default_lock_materializer(worker, epoch),
    do: Materializer.lock_for_recovery(worker, epoch, timeout_in_ms: 30_000)

  defp unlock_and_start_pulling(pid, node, recovery_info, sources, context) do
    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(pid, start_version(recovery_info), sources) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unlock_failed, reason, node}}
      {:failure, reason, _ref} -> {:error, {:unlock_failed, reason, node}}
    end
  end

  # The unlock version tells the materializer which version its own store
  # already reflects — it pulls from AFTER that point. A recruited worker
  # is freshly created (empty), so the version IT reports at lock time
  # (zero for a new store) is the only honest choice: any cluster-wide
  # version would silently skip everything before it.
  defp start_version(recovery_info) when is_map(recovery_info),
    do: Map.get(recovery_info, :durable_version) || Version.zero()

  defp start_version(recovery_info) when is_list(recovery_info),
    do: Keyword.get(recovery_info, :durable_version) || Version.zero()

  defp start_version(_recovery_info), do: Version.zero()

  defp default_unlock_materializer(pid, durable_version, pull_sources),
    do: Materializer.unlock_after_recovery(pid, durable_version, pull_sources, timeout_in_ms: 30_000)

  # The typed seed: this shard's replica set as {log_id, log_ref} pairs,
  # resolved through the same ShardRouter walk proxies route with and
  # bootstrap seeds with — the single placement site. A materializer
  # unlocked with an empty replica set would be published as covered yet
  # never advance a version — fail loudly BEFORE any worker exists
  # instead of manufacturing a silent black hole.
  defp pull_sources_for_shard(tag, %{logs: logs, log_refs: log_refs}) do
    sources =
      tag
      |> ShardRouter.log_ids_for_tag(ShardRouter.log_map(Map.keys(logs)), max(1, map_size(logs)))
      |> Enum.flat_map(fn log_id ->
        case Map.get(log_refs, log_id) do
          nil -> []
          ref -> [{log_id, ref}]
        end
      end)

    case sources do
      [] -> {:error, {:no_pull_sources, tag}}
      sources -> {:ok, sources}
    end
  end
end
