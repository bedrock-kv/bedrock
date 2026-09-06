defmodule Bedrock.Service.Foreman.Impl do
  @moduledoc false
  import Bedrock.Service.Foreman.Health,
    only: [compute_health_from_worker_info: 1]

  import Bedrock.Service.Foreman.StartingWorkers,
    only: [
      abandoned_paths_from_disk: 1,
      worker_info_from_path: 2,
      try_to_start_workers: 3,
      try_to_start_worker: 3,
      initialize_new_worker: 5
    ]

  import Bedrock.Service.Foreman.State

  alias Bedrock.Cluster
  alias Bedrock.Cluster.Link
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.Service.Foreman.Removal
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Worker

  require Logger

  @spec do_fetch_workers(State.t()) :: [Worker.ref()]
  def do_fetch_workers(t), do: otp_names_for_running_workers(t)

  @spec do_fetch_materializer_workers(State.t()) :: [Worker.ref()]
  def do_fetch_materializer_workers(t), do: otp_names_for_running_materializer_workers(t)

  @spec do_get_all_running_services(State.t()) :: [{:log | :materializer, atom()}]
  def do_get_all_running_services(t) do
    t.workers
    |> Enum.filter(fn {_id, worker_info} ->
      worker_healthy?(worker_info) and worker_info.manifest != nil
    end)
    |> Enum.map(fn {_id, worker_info} -> compact_service_info_from_worker_info(worker_info) end)
  end

  @doc """
  Relays a newly durable transaction system layout to every running
  hosted worker.

  The foreman never answers a membership question: workers self-detect
  displacement against the pushed layout (FDB-style — every role decides
  its own retirement; no component decides another process's). One
  foreman per node makes it the natural distribution point from the
  cluster push to the workers it hosts.

  Bounded by liveness: only a running worker can self-detect, so a worker
  that cannot start (crash loop, corrupt state) keeps its directory until
  an operator intervenes — the same property FDB has for a process that
  cannot run its rejoin check. A nil layout (the coordinator clears and
  broadcasts nil when a director starts) carries nothing to judge against
  and is not relayed.
  """
  @spec do_relay_tsl(State.t(), TransactionSystemLayout.t() | nil) :: State.t()
  def do_relay_tsl(t, nil), do: t

  def do_relay_tsl(t, transaction_system_layout) do
    for {_id, %{health: {:ok, pid}}} <- t.workers do
      send(pid, {:tsl_updated, transaction_system_layout})
    end

    t
  end

  @doc """
  Janitors a worker that decided its own retirement.

  The worker found itself displaced from the committed layout and is
  exiting (`:transient` restart means a deliberate shutdown stays down);
  the foreman disposes what remains — process (if still up),
  registration, directory, manifest — and does not restart it.
  """
  @spec do_worker_retired(State.t(), Worker.id()) :: State.t()
  def do_worker_retired(t, worker_id) do
    Logger.info("Bedrock foreman: worker #{worker_id} retired itself; disposing")
    {t, result} = do_remove_worker(t, worker_id)

    if result != :ok,
      do: Logger.warning("Bedrock foreman: retirement cleanup for #{worker_id} deferred: #{inspect(result)}")

    t
  end

  @spec do_worker_retired(State.t(), Worker.id(), pid()) :: State.t()
  def do_worker_retired(t, worker_id, reporter) do
    case Map.get(t.workers, worker_id) do
      %WorkerInfo{} = info ->
        registered = registered_worker(info)

        if is_pid(reporter) and
             (registered == reporter or
                (is_nil(registered) and incarnation(info) == reporter)), do: do_worker_retired(t, worker_id), else: t

      nil ->
        t
    end
  end

  defp registered_worker(%{otp_name: name}) when is_atom(name) and not is_nil(name), do: Process.whereis(name)
  defp registered_worker(_), do: nil
  defp incarnation(%{incarnation_pid: pid}) when is_pid(pid), do: pid
  defp incarnation(%{health: {:ok, pid}}), do: pid
  defp incarnation(_), do: nil

  @spec do_new_worker(State.t(), Worker.id(), :log | :materializer, params :: map()) ::
          {State.t(), Worker.ref()}
  def do_new_worker(t, id, kind, params \\ %{}) do
    # An id already in the map means a retried creation — a director that
    # timed out and asked again. The entry is about to be overwritten, so
    # release its monitor first or the ref leaks: nothing else can reach
    # it once the entry is gone.
    release_monitor(Map.get(t.workers, id))

    worker_info =
      id
      |> initialize_new_worker(worker_for_kind(kind), params, t.path, t.cluster)
      |> try_to_start_worker(t.cluster, t.object_storage)
      |> advertise_running_worker(t.cluster)
      |> monitor_worker()

    t =
      t
      |> update_workers(&Map.put(&1, id, worker_info))
      |> recompute_health()

    {t, worker_info.otp_name}
  end

  @spec do_remove_worker(State.t(), Worker.id()) ::
          {State.t(),
           :ok
           | {:error,
              :worker_not_found | :worker_shutdown_unresolved | {:failed_to_remove_directory, File.posix(), Path.t()}}}
  def do_remove_worker(t, worker_id) do
    case Map.get(t.workers, worker_id) do
      nil ->
        {t, {:error, :worker_not_found}}

      worker_info ->
        case remove_worker_completely(worker_info, t) do
          :ok ->
            {t |> update_workers(&Map.delete(&1, worker_id)) |> recompute_health(), :ok}

          {:error, :worker_shutdown_unresolved} = error ->
            {t, error}

          error ->
            stopped = worker_info |> WorkerInfo.put_health(:stopped) |> WorkerInfo.put_monitor_ref(nil)
            {t |> update_workers(&Map.put(&1, worker_id, stopped)) |> recompute_health(), error}
        end
    end
  end

  @spec do_remove_workers(State.t(), [Worker.id()]) ::
          {State.t(),
           %{
             Worker.id() =>
               :ok
               | {:error,
                  :worker_not_found
                  | :worker_shutdown_unresolved
                  | {:failed_to_remove_directory, File.posix(), Path.t()}}
           }}
  def do_remove_workers(t, worker_ids) do
    {updated_state, results} = process_worker_removals(t, worker_ids)
    final_state = recompute_health(updated_state)
    {final_state, results}
  end

  defp process_worker_removals(initial_state, worker_ids) do
    Enum.reduce(worker_ids, {initial_state, %{}}, &remove_single_worker/2)
  end

  defp remove_single_worker(worker_id, {state, acc_results}) do
    {state, result} = do_remove_worker(state, worker_id)
    {state, Map.put(acc_results, worker_id, result)}
  end

  @spec advertise_running_workers([WorkerInfo.t()], Cluster.t()) :: [WorkerInfo.t()]
  def advertise_running_workers(worker_infos, cluster) do
    Enum.each(worker_infos, &advertise_running_worker(&1, cluster))
    worker_infos
  end

  @spec advertise_running_worker(WorkerInfo.t(), module()) :: WorkerInfo.t()
  def advertise_running_worker(%{health: {:ok, pid}} = worker_info, cluster) do
    # Get coordinator from link
    link = cluster.otp_name(:link)

    case Link.fetch_coordinator(link) do
      {:ok, coordinator} ->
        # Get worker info and register directly with coordinator
        case Worker.info(pid, [:id, :otp_name, :kind, :pid]) do
          {:ok, info} ->
            service_info = {info[:id], info[:kind], {info[:otp_name], Node.self()}}
            Coordinator.register_services(coordinator, [service_info])

          _ ->
            :ok
        end

      _ ->
        :ok
    end

    worker_info
  end

  @spec advertise_running_worker(WorkerInfo.t(), module()) :: WorkerInfo.t()
  def advertise_running_worker(worker_info, _), do: worker_info

  @spec remove_worker_completely(WorkerInfo.t(), State.t()) ::
          :ok | {:error, :worker_shutdown_unresolved | {:failed_to_remove_directory, File.posix(), Path.t()}}
  defp remove_worker_completely(worker_info, t) do
    with :ok <- Removal.stop(t, worker_info) do
      release_monitor(worker_info)
      :ok = unadvertise_worker(worker_info, t.cluster)
      cleanup_worker_directory(worker_info, t.path)
    end
  end

  @spec release_monitor(WorkerInfo.t() | nil) :: :ok
  defp release_monitor(nil), do: :ok
  defp release_monitor(%{monitor_ref: nil}), do: :ok

  defp release_monitor(%{monitor_ref: ref}) do
    Process.demonitor(ref, [:flush])
    :ok
  end

  # Best-effort: a ghost directory entry is tolerable (locking a gone
  # service fails and is skipped), but leaving one behind on every
  # retirement would accrete forever.
  @spec unadvertise_worker(WorkerInfo.t(), module()) :: :ok
  defp unadvertise_worker(%{id: worker_id}, cluster) do
    case Link.fetch_coordinator(cluster.otp_name(:link)) do
      {:ok, coordinator} ->
        _ = Coordinator.deregister_services(coordinator, [worker_id])
        :ok

      _ ->
        :ok
    end
  catch
    _, _ -> :ok
  end

  @spec cleanup_worker_directory(WorkerInfo.t(), String.t()) ::
          :ok | {:error, {:failed_to_remove_directory, File.posix(), Path.t()}}
  defp cleanup_worker_directory(%{id: worker_id}, base_path) do
    worker_path = Path.join(base_path, worker_id)

    case File.rm_rf(worker_path) do
      {:ok, _files_and_directories} -> :ok
      {:error, reason, file} -> {:error, {:failed_to_remove_directory, reason, file}}
    end
  end

  @doc """
  Records that a monitored worker's process is gone.

  The process is gone right now, so `:stopped` is what is true right now
  — and it is the state the worker held before it ever started, so no
  health value the fold does not already understand.

  It may not stay true. Workers are `:transient` under a
  DynamicSupervisor, so an abnormal exit is restarted under the same OTP
  name, and leaving the worker at `:stopped` would drop a LIVE worker out
  of the roll call `do_get_all_running_services/1` advertises to the
  coordinator — the original bug's mirror image. Adopting that
  replacement cannot happen here, because the `:DOWN` always beats the
  restart: it is delivered the instant the process dies, while the
  supervisor must first handle its own `EXIT`. The caller therefore
  schedules `do_worker_recheck/3`, and the worker id is returned for
  exactly that purpose.

  A `:DOWN` whose ref matches no worker is ignored, which is the ordinary
  case for one the foreman deliberately removed:
  `remove_worker_completely/3` demonitors with `:flush`, so a queued
  `:DOWN` is discarded before it can be mistaken for a death.
  """
  @spec do_worker_down(State.t(), reference(), term()) ::
          {State.t(), Worker.id() | :no_such_worker}
  def do_worker_down(t, ref, reason) do
    case Enum.find(t.workers, fn {_id, worker_info} -> worker_info.monitor_ref == ref end) do
      nil -> {t, :no_such_worker}
      {worker_id, _worker_info} -> {resettle_worker(t, worker_id, reason), worker_id}
    end
  end

  @spec resettle_worker(State.t(), Worker.id(), term()) :: State.t()
  defp resettle_worker(t, worker_id, reason) do
    Logger.warning("Bedrock foreman: worker #{worker_id} died (#{inspect(reason)})")

    t
    |> update_workers(
      &Map.update!(&1, worker_id, fn info ->
        info |> WorkerInfo.put_health(:stopped) |> WorkerInfo.put_monitor_ref(nil)
      end)
    )
    |> recompute_health()
  end

  @doc """
  Adopts the replacement a supervisor started for a worker that died.

  Split from `do_worker_down/3` because the two events race and the
  `:DOWN` always wins: it is delivered the instant the process dies,
  while the supervisor must first handle its own `EXIT` and only then
  start a child. Re-resolving at `:DOWN` time therefore finds nothing
  almost every time, which would leave a worker the supervisor DID
  restart parked at `:stopped` — dropping a live worker out of the roll
  call `do_get_all_running_services/1` advertises from, which is the
  original bug's mirror image.

  Only a worker still `:stopped` is adopted, so this can never overwrite
  a health the worker reported for itself in the meantime. Attempts are
  bounded: a `:transient` child that exited normally is never coming
  back, and retrying forever would mean a timer per dead worker for the
  life of the node. A restart slower than that window therefore leaves a
  live worker recorded as `:stopped` until the next spin-up
  (bedrock-gu0.2) — stale toward unhealthy, which is the safe direction.
  """
  @spec do_worker_recheck(State.t(), Worker.id(), non_neg_integer()) ::
          {State.t(), :done | :retry}
  def do_worker_recheck(t, worker_id, attempts_left) do
    if Map.has_key?(t.workers, worker_id), do: recheck_existing_worker(t, worker_id, attempts_left), else: {t, :done}
  end

  defp recheck_existing_worker(t, worker_id, attempts_left) do
    with %{health: :stopped, otp_name: otp_name} <- Map.get(t.workers, worker_id),
         pid when is_pid(pid) <- Process.whereis(otp_name) do
      Logger.info("Bedrock foreman: worker #{worker_id} was replaced; adopting #{inspect(pid)}")

      t =
        t
        |> update_workers(
          &Map.update!(&1, worker_id, fn info ->
            info |> WorkerInfo.put_health({:ok, pid}) |> monitor_worker()
          end)
        )
        |> recompute_health()

      {t, :done}
    else
      _ when attempts_left > 0 -> {t, :retry}
      _ -> {t, :done}
    end
  end

  @spec do_worker_health(State.t(), Worker.id(), pid(), WorkerInfo.health()) :: State.t()
  def do_worker_health(t, worker_id, reporter, health) do
    with %WorkerInfo{} = info <- Map.get(t.workers, worker_id),
         true <- is_pid(reporter) and registered_worker(info) == reporter,
         true <- reported_pid_matches?(health, reporter) do
      t = update_workers(t, &Map.update!(&1, worker_id, fn info -> %{info | incarnation_pid: reporter} end))
      do_worker_health(t, worker_id, health)
    else
      _ -> t
    end
  end

  defp reported_pid_matches?({:ok, pid}, reporter), do: pid == reporter
  defp reported_pid_matches?(_health, _reporter), do: true

  @spec do_worker_health(State.t(), Worker.id(), WorkerInfo.health()) :: State.t()
  def do_worker_health(t, worker_id, health) do
    t
    |> put_health_for_worker(worker_id, health)
    |> rewatch_worker(worker_id, Map.get(t.workers, worker_id))
    |> recompute_health()
  end

  # A worker reporting its own health can name a DIFFERENT process than
  # the one being watched. Olivine's replacement casts {:ok, self()} the
  # moment its startup completes, which for a small shard beats the
  # recheck timer — and `do_worker_recheck/3` only adopts a worker still
  # `:stopped`, so it would then retry to exhaustion and give up. The new
  # pid would be left carrying a stale ref or none at all, no further
  # :DOWN would ever arrive for it, and the worker would be back in
  # exactly the state this whole change exists to eliminate.
  @spec rewatch_worker(State.t(), Worker.id(), WorkerInfo.t() | nil) :: State.t()
  defp rewatch_worker(t, worker_id, previous) do
    update_workers(
      t,
      &Map.update!(&1, worker_id, fn info ->
        if watching?(previous, info.health) do
          info
        else
          release_monitor(previous)
          info |> WorkerInfo.put_monitor_ref(nil) |> monitor_worker()
        end
      end)
    )
  end

  @spec watching?(WorkerInfo.t() | nil, WorkerInfo.health()) :: boolean()
  defp watching?(%{monitor_ref: ref, health: {:ok, pid}}, {:ok, pid}) when is_reference(ref), do: true
  defp watching?(_previous, _health), do: false

  @spec do_spin_up(State.t()) :: State.t()
  def do_spin_up(t) do
    t
    |> report_abandoned_directories()
    |> load_workers_from_disk()
    |> start_workers_that_are_stopped()
    |> relay_current_tsl()
    # Spin-up is where the foreman learns what it has and starts it, so
    # it is where the verdict has to be settled. Without this the state
    # keeps the :starting it was constructed with: recompute_health/1 is
    # otherwise reachable only from a worker's own health cast, and the
    # sole sender is Olivine — Shale never reports. A log-only node
    # therefore had no path off :starting at all.
    |> recompute_health()
  end

  # An unstartable directory is silent by nature: with no manifest there
  # is no worker process, so nothing can report its own health and
  # nothing can retire itself. Left unreported it is invisible — retried
  # and re-failed on every boot while holding its disk. Say it out loud
  # at the one moment an operator is looking, and name the paths so the
  # cleanup is a copy-paste. Reporting only; a WAL is real data and the
  # foreman does not delete on a guess.
  @spec report_abandoned_directories(State.t()) :: State.t()
  defp report_abandoned_directories(t) do
    case abandoned_paths_from_disk(t.path) do
      [] ->
        t

      paths ->
        Logger.warning(
          "Bedrock foreman: #{length(paths)} abandoned working " <>
            "#{if length(paths) == 1, do: "directory", else: "directories"} under #{t.path} " <>
            "(no manifest, so they cannot be started and cannot retire themselves). " <>
            "They will be ignored on every boot until removed by hand: " <>
            Enum.map_join(paths, ", ", &Path.basename/1)
        )

        t
    end
  end

  # Cold boot composes with self-detection only if resurrected workers
  # see a layout. The coordinator replays its push on Link subscription,
  # which can precede this foreman (the forward is dropped when whereis
  # finds no foreman), so pull the Link's cached layout once at spin-up —
  # rehydrated workers self-validate immediately instead of waiting for
  # the next recovery's push.
  @spec relay_current_tsl(State.t()) :: State.t()
  defp relay_current_tsl(t) do
    case Link.fetch_transaction_system_layout(t.cluster.otp_name(:link)) do
      {:ok, transaction_system_layout} -> do_relay_tsl(t, transaction_system_layout)
      _ -> t
    end
  catch
    _, _ -> t
  end

  @spec load_workers_from_disk(State.t()) :: State.t()
  def load_workers_from_disk(t) do
    update_workers(t, fn workers ->
      t.path
      |> worker_info_from_path(&t.cluster.otp_name_for_worker(&1))
      |> merge_worker_info_into_workers(workers)
    end)
  end

  @spec start_workers_that_are_stopped(State.t()) :: State.t()
  def start_workers_that_are_stopped(t) do
    update_workers(t, fn workers ->
      workers
      |> Map.values()
      |> Enum.filter(&(&1.health == :stopped))
      |> try_to_start_workers(t.cluster, t.object_storage)
      |> advertise_running_workers(t.cluster)
      |> Enum.map(&monitor_worker/1)
      |> merge_worker_info_into_workers(workers)
    end)
  end

  @doc """
  Watches a running worker so its death is observable.

  A worker's health is otherwise recorded once, at start, and never
  revisited — so `{:ok, pid}` outlives the process it names. The foreman
  would go on relaying layout pushes to a dead worker, advertising it to
  the coordinator as a running service, and folding it into a verdict of
  `:ok`.

  The monitor must be taken by the foreman itself. Starting happens
  inside a `Task.async_stream`, and a monitor established there would
  belong to the task and die with it.
  """
  @spec monitor_worker(WorkerInfo.t()) :: WorkerInfo.t()
  def monitor_worker(%{health: {:ok, pid}, monitor_ref: nil} = worker_info),
    do: WorkerInfo.put_monitor_ref(worker_info, Process.monitor(pid))

  def monitor_worker(worker_info), do: worker_info

  @spec recompute_health(State.t()) :: State.t()
  def recompute_health(t) do
    put_health(t, t.workers |> Map.values() |> compute_health_from_worker_info())
  end

  @spec merge_worker_info_into_workers(
          [WorkerInfo.t()],
          workers :: %{Worker.id() => WorkerInfo.t()}
        ) ::
          %{Worker.id() => WorkerInfo.t()}
  defp merge_worker_info_into_workers(worker_info, workers), do: Enum.into(worker_info, workers, &{&1.id, &1})

  @spec worker_for_kind(:log) :: module()
  defp worker_for_kind(:log), do: Bedrock.DataPlane.Log.Shale

  @spec worker_for_kind(:materializer) :: module()
  defp worker_for_kind(:materializer), do: Bedrock.DataPlane.Materializer.Olivine

  @spec otp_names_for_running_workers(State.t()) :: [atom()]
  def otp_names_for_running_workers(t), do: Enum.map(t.workers, fn {_id, %{otp_name: otp_name}} -> otp_name end)

  @spec otp_names_for_running_materializer_workers(State.t()) :: [atom()]
  def otp_names_for_running_materializer_workers(t) do
    t.workers
    |> Enum.filter(fn {_id, worker_info} ->
      materializer_worker?(worker_info)
    end)
    |> Enum.map(fn {_id, %{otp_name: otp_name}} -> otp_name end)
  end

  @spec materializer_worker?(WorkerInfo.t()) :: boolean()
  def materializer_worker?(%{manifest: %{worker: worker}}) when not is_nil(worker), do: worker.kind() == :materializer

  def materializer_worker?(_), do: false

  @spec worker_healthy?(WorkerInfo.t()) :: boolean()
  def worker_healthy?(%{health: {:ok, _pid}}), do: true
  def worker_healthy?(_), do: false

  @spec compact_service_info_from_worker_info(WorkerInfo.t()) ::
          {String.t(), :log | :materializer, atom()}
  def compact_service_info_from_worker_info(%{id: id, manifest: %{worker: worker}, otp_name: otp_name}) do
    kind = worker.kind()
    {id, kind, otp_name}
  end

  @spec service_info_from_worker_info(WorkerInfo.t()) ::
          {String.t(), :log | :materializer, {atom(), node()}}
  def service_info_from_worker_info(%{id: id, manifest: %{worker: worker}, otp_name: otp_name}) do
    kind = worker.kind()
    worker_ref = {otp_name, Node.self()}
    {id, kind, worker_ref}
  end
end
