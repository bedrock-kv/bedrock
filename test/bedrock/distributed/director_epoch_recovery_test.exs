defmodule Bedrock.Distributed.DirectorEpochRecoveryTest do
  use ExUnit.Case, async: false

  alias Bedrock.ClusterBootstrap.Publication
  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.RecoveryControl

  defmodule Cluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "director_epoch_recovery"
  end

  defmodule Repo do
    use Bedrock.Repo, cluster: Cluster
  end

  @moduletag :distributed
  @moduletag timeout: 60_000

  test "a log crash under a live coordinator recovers through a fresh fenced generation" do
    assert Node.alive?(), "run with elixir --sname director_epoch_test -S mix run --no-start"

    root =
      Path.join(
        System.tmp_dir!(),
        "director-epoch-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}"
      )

    object_root = Path.join(root, "objects")
    worker_root = Path.join(root, "workers")
    coordinator_root = Path.join(root, "coordinator")
    File.mkdir_p!(root)

    previous = Application.get_env(:bedrock, Cluster)
    previous_storage = Application.get_env(:bedrock, Bedrock.ObjectStorage)
    backend = {Bedrock.ObjectStorage.LocalFilesystem, root: object_root}
    Application.put_env(:bedrock, Bedrock.ObjectStorage, backend: backend)

    Application.put_env(:bedrock, Cluster,
      capabilities: [:coordination, :log, :materializer],
      durability_mode: :relaxed,
      path_to_descriptor: Path.join(root, "descriptor"),
      object_storage: backend,
      coordinator: [path: coordinator_root],
      materializer: [path: worker_root, object_storage: backend],
      log: [path: worker_root, object_storage: backend]
    )

    start_supervised!({Cluster, []})

    on_exit(fn ->
      if previous,
        do: Application.put_env(:bedrock, Cluster, previous),
        else: Application.delete_env(:bedrock, Cluster)

      if previous_storage,
        do: Application.put_env(:bedrock, Bedrock.ObjectStorage, previous_storage),
        else: Application.delete_env(:bedrock, Bedrock.ObjectStorage)
    end)

    zero = <<0::little-unsigned-64>>
    one = <<1::little-unsigned-64>>

    assert :ok =
             Repo.transact(
               fn ->
                 Repo.put("epoch/prefix", "acknowledged")
                 Repo.put("epoch/tail-counter", zero)
               end,
               timeout_in_ms: 15_000
             )

    assert "acknowledged" = Repo.transact(fn -> Repo.get("epoch/prefix") end, timeout_in_ms: 15_000)

    initial_layout = Cluster.transaction_system_layout!()
    coordinator = Process.whereis(Cluster.otp_name(:coordinator))
    initial_coordinator = :sys.get_state(coordinator)
    old_director = initial_coordinator.director
    old_publication = publication_identity(initial_coordinator)
    initial_director = :sys.get_state(old_director)
    old_bootstrap = read_bootstrap!(backend)
    old_authority = authority!(initial_coordinator.bootstrap_reservation)
    old_config_sequence = initial_coordinator.publication_sequences.config
    assert old_config_sequence > 0
    assert :ok = completed_bootstrap?(old_bootstrap, old_authority)
    old_components = old_transient_components(initial_layout, old_director, initial_director.distributor)
    {:ok, events} = Agent.start_link(fn -> [] end)
    trace = {__MODULE__, self()}

    :ok =
      :telemetry.attach_many(
        trace,
        [[:bedrock, :log, :push], [:bedrock, :control_plane, :coordinator, :director_launch]],
        &__MODULE__.trace/4,
        {self(), events}
      )

    try do
      tail =
        Task.async(fn ->
          try do
            Repo.transact(
              fn ->
                Repo.add("epoch/tail-counter", one)
                Repo.put("epoch/tail-marker", "ambiguous")
              end,
              retry_limit: 0,
              timeout_in_ms: 1_000
            )

            :committed
          rescue
            error -> {:unknown, Exception.message(error)}
          catch
            kind, reason -> {:unknown, {kind, reason}}
          end
        end)

      assert_receive {:synced_tail, log, token, encoded}, 5_000
      assert {:ok, transaction} = Transaction.decode(encoded)
      assert {:atomic, :add, "epoch/tail-counter", one} in transaction.mutations
      assert {:set, "epoch/tail-marker", "ambiguous"} in transaction.mutations
      assert {:ok, tail_version} = Transaction.commit_version(encoded)
      finalizer = capture_finalizer!(initial_layout.proxies)
      finalizer_down = Process.monitor(finalizer)

      log_down = Process.monitor(log)
      Process.exit(log, :kill)
      assert_receive {:DOWN, ^log_down, :process, ^log, :killed}, 5_000
      send(log, {:release, token})

      assert {:unknown, _} = tail_outcome = Task.await(tail, 5_000)
      assert_receive {:director_launch, replacement_generation}, 5_000
      assert_receive {:DOWN, ^finalizer_down, :process, ^finalizer, _}, 5_000

      assert {:ok, recovered} =
               eventually(fn -> recovered_state(coordinator, initial_layout, old_components, backend) end, 15_000)

      assert Process.whereis(Cluster.otp_name(:coordinator)) == coordinator
      assert recovered.coordinator.leader_node == initial_coordinator.leader_node
      assert recovered.coordinator.raft_term == initial_coordinator.raft_term
      assert recovered.coordinator.director != old_director
      assert replacement_generation == recovered.layout.epoch
      assert replacement_generation > initial_layout.epoch
      assert recovered.authority.recovery_id != old_authority.recovery_id
      assert {"acknowledged", tail_counter, tail_marker} = recovered.readback
      assert {tail_counter, tail_marker} in [{zero, nil}, {one, "ambiguous"}]
      assert Enum.all?(old_components, &(not Process.alive?(&1)))
      assert %{generation: replacement_generation, recovery_id: recovery_id} = recovered.authority
      assert is_binary(recovery_id) and byte_size(recovery_id) > 0
      assert Enum.any?(recovered.workers, &(&1.kind == :log))
      assert Enum.any?(recovered.workers, &(&1.kind == :materializer))
      assert Enum.all?(recovered.workers, &(&1.authority == recovered.authority))
      assert Enum.all?(recovered.workers, &(&1.control.phase == :running))
      assert Enum.all?(recovered.workers, &(external_authority(&1.control.authority) == recovered.authority))
      assert retired_logs_absent?(initial_layout, recovered.layout)

      assert {:ok, current_coordinator} = eventually(fn -> coherent_checkpoint_state(coordinator) end, 2_000)
      assert current_coordinator.transaction_system_layout == recovered.layout
      allocation_evidence = assert_allocation_and_checkpoint!(current_coordinator, recovered.authority)
      assert_completed_bootstrap!(recovered.bootstrap, recovered.authority, recovered.layout)
      assert current_coordinator.bootstrap_reservation.prior_bootstrap == old_bootstrap.bootstrap

      tail_history = retained_history(recovered.workers, encoded, tail_version)
      expected_occurrences = if tail_counter == one, do: 1, else: 0
      assert tail_history.occurrences == expected_occurrences
      assert tail_history.conflicting_versions == []

      assert :ok = Repo.transact(fn -> Repo.put("epoch/after", "live") end, timeout_in_ms: 5_000)
      assert "live" = Repo.transact(fn -> Repo.get("epoch/after") end, timeout_in_ms: 5_000)

      stable = stable_snapshot(coordinator, recovered.workers, backend)

      stale_results =
        reject_stale_storage_work!(
          recovered.workers,
          old_authority,
          %{generation: recovered.authority.generation, recovery_id: "foreign-#{old_authority.recovery_id}"}
        )

      deliver_old_publication(coordinator, old_director, initial_layout, initial_coordinator, old_publication)

      GenServer.cast(
        coordinator,
        {:notify_config, {old_director, initial_layout.epoch, old_config_sequence}, initial_coordinator.config}
      )

      GenServer.cast(
        coordinator,
        {:notify_transaction_system_layout, initial_layout, initial_coordinator.prior_core_state}
      )

      GenServer.cast(coordinator, {:notify_config, initial_coordinator.config})
      after_stale = :sys.get_state(coordinator)
      assert after_stale.transaction_system_layout == recovered.layout
      assert Cluster.transaction_system_layout!() == recovered.layout

      assert {:error, :publication_mismatch} =
               Publication.publish(initial_coordinator.bootstrap_reservation, old_bootstrap.bootstrap)

      assert_stable_snapshot!(stable, coordinator, recovered.workers, backend)

      evidence = %{
        coordinator_unchanged: coordinator,
        initial_generation: initial_layout.epoch,
        replacement_generation: replacement_generation,
        recovery_authority: recovered.authority,
        initial_logs: Map.keys(initial_layout.logs),
        recovered_logs: Map.keys(recovered.layout.logs),
        acknowledged_prefix: elem(recovered.readback, 0),
        ambiguous_tail_counter: tail_counter,
        ambiguous_tail_marker: tail_marker,
        ambiguous_tail_version: tail_version,
        tail_history: tail_history,
        tail_outcome: tail_outcome,
        old_components_alive: Enum.map([finalizer | old_components], &{&1, Process.alive?(&1)}),
        stale_publication_rejected: after_stale.transaction_system_layout == recovered.layout,
        stale_storage_results: stale_results,
        allocation: allocation_evidence,
        checkpoint: checkpoint(current_coordinator),
        finalizer_at_gate: finalizer,
        generation_a: %{
          authority: old_authority,
          layout_sequence: elem(old_publication, 2),
          config_sequence: old_config_sequence,
          bootstrap_version_token: old_bootstrap.version_token,
          bootstrap_bytes_sha256: Base.encode16(:crypto.hash(:sha256, old_bootstrap.bytes))
        },
        storage_b:
          Enum.map(recovered.workers, fn worker ->
            %{
              id: worker.id,
              kind: worker.kind,
              pid: worker.pid,
              path: worker.path,
              authority: worker.authority,
              control: worker.control,
              control_bytes_sha256: Base.encode16(:crypto.hash(:sha256, worker.control_bytes))
            }
          end),
        bootstrap_identity:
          Map.take(recovered.bootstrap.bootstrap, [:epoch, :recovery_generation, :recovery_id, :publication_id]),
        events: Agent.get(events, &Enum.reverse/1)
      }

      artifact = Path.join(root, "recovery.term")
      File.write!(artifact, :erlang.term_to_binary(evidence))
      File.write!(artifact <> ".txt", inspect(evidence, pretty: true, limit: :infinity))
      IO.puts("Director recovery artifact: #{artifact}")
    after
      :telemetry.detach(trace)
    end
  end

  def trace(event, _measurements, metadata, {owner, events}) do
    Agent.update(events, &[{System.monotonic_time(), self(), event, metadata} | &1])

    case {event, metadata} do
      {[:bedrock, :control_plane, :coordinator, :director_launch], %{epoch: epoch}} ->
        send(owner, {:director_launch, epoch})

      {[:bedrock, :log, :push], %{transaction: encoded}} ->
        {:ok, decoded} = Transaction.decode(encoded)

        if {:set, "epoch/tail-marker", "ambiguous"} in decoded.mutations do
          token = make_ref()
          send(owner, {:synced_tail, self(), token, encoded})

          receive do
            {:release, ^token} -> :ok
          after
            5_000 -> :ok
          end
        end

      _ ->
        :ok
    end
  end

  defp old_transient_components(layout, director, distributor) do
    resolvers =
      Enum.map(layout.resolvers, fn
        %{resolver: resolver} -> resolver
        {_start_key, resolver} -> resolver
      end)

    assert layout.proxies != []
    assert resolvers != []

    components = [director, distributor, layout.sequencer | layout.proxies ++ resolvers]
    assert Enum.all?(components, &(is_pid(&1) and Process.alive?(&1)))
    Enum.uniq(components)
  end

  defp capture_finalizer!(proxies) do
    assert {:ok, finalizer} =
             eventually(
               fn ->
                 tasks =
                   Enum.flat_map(proxies, fn proxy ->
                     proxy |> :sys.get_state(1_000) |> Map.fetch!(:finalization_tasks) |> Map.keys()
                   end)

                 case Enum.uniq(tasks) do
                   [pid] -> {:ok, pid}
                   _ -> :retry
                 end
               end,
               2_000
             )

    finalizer
  end

  defp recovered_state(coordinator, initial_layout, old_components, backend) do
    state = :sys.get_state(coordinator)

    with director when is_pid(director) <- state.director,
         true <- Process.alive?(director),
         true <- director not in old_components,
         %{epoch: generation} = layout <- state.transaction_system_layout,
         true <- layout.logs != initial_layout.logs,
         false <- Enum.any?(old_components, &Process.alive?/1),
         {"acknowledged", tail, marker} = readback <- public_readback(),
         true <- legal_tail?({tail, marker}),
         authority = reservation_authority(Map.get(state, :bootstrap_reservation), generation),
         %{generation: ^generation} <- authority,
         {:ok, bootstrap} <- Publication.read(backend, "bootstrap"),
         :ok <- completed_bootstrap?(bootstrap, authority),
         {:ok, workers} <- current_workers(layout, authority) do
      {:ok,
       %{
         coordinator: state,
         layout: layout,
         readback: readback,
         authority: authority,
         bootstrap: bootstrap,
         workers: workers
       }}
    else
      _ -> :retry
    end
  end

  defp public_readback do
    Repo.transact(
      fn ->
        {Repo.get("epoch/prefix"), Repo.get("epoch/tail-counter"), Repo.get("epoch/tail-marker")}
      end,
      retry_limit: 0,
      timeout_in_ms: 1_000
    )
  rescue
    _ -> :unavailable
  catch
    _, _ -> :unavailable
  end

  defp current_workers(layout, authority) do
    {:ok, workers} = Foreman.get_all_running_services(Cluster.otp_name(:foreman))

    workers
    |> Enum.filter(fn {id, kind, _name} -> kind == :materializer or Map.has_key?(layout.logs, id) end)
    |> Enum.reduce_while({:ok, []}, fn {id, kind, name}, {:ok, acc} ->
      with pid when is_pid(pid) <- Process.whereis(name),
           worker_state = :sys.get_state(pid, 1_000),
           :running <- Map.get(worker_state, :mode),
           ^authority <- external_authority(Map.get(worker_state, :recovery_authority)),
           path when is_binary(path) <- Map.get(worker_state, :path),
           {:ok, control_bytes} <- File.read(RecoveryControl.path(path)),
           {:ok, control} <- RecoveryControl.decode(control_bytes),
           :running <- control.phase,
           ^authority <- external_authority(control.authority) do
        entry = %{
          id: id,
          kind: kind,
          name: name,
          pid: pid,
          path: path,
          authority: authority,
          control: control,
          control_bytes: control_bytes,
          state: worker_state
        }

        {:cont, {:ok, [entry | acc]}}
      else
        _ -> {:halt, :retry}
      end
    end)
  end

  defp completed_bootstrap?(loaded, authority) do
    b = loaded.bootstrap

    if b.epoch == authority.generation and b.recovery_generation == authority.generation and
         b.recovery_id == authority.recovery_id and b.publication_id == authority.recovery_id,
       do: :ok,
       else: :retry
  end

  defp read_bootstrap!(backend) do
    assert {:ok, loaded} = Publication.read(backend, "bootstrap")
    loaded
  end

  defp authority!(%{generation: generation, recovery_id: recovery_id}),
    do: %{generation: generation, recovery_id: recovery_id}

  defp assert_completed_bootstrap!(loaded, authority, layout) do
    assert :ok = completed_bootstrap?(loaded, authority)
    assert Enum.sort(Enum.map(loaded.bootstrap.logs, & &1.id)) == Enum.sort(Map.keys(layout.logs))
    assert CoreState.from_bootstrap(loaded.bootstrap).logs == layout.logs
  end

  defp retired_logs_absent?(initial_layout, recovered_layout) do
    {:ok, workers} = Foreman.get_all_running_services(Cluster.otp_name(:foreman))
    current_ids = MapSet.new(workers, &elem(&1, 0))

    initial_layout.logs
    |> Map.keys()
    |> Enum.reject(&Map.has_key?(recovered_layout.logs, &1))
    |> Enum.all?(&(not MapSet.member?(current_ids, &1)))
  end

  defp checkpoint(state) do
    raft_log = Bedrock.Raft.log(state.raft)
    [{:coordinator_checkpoint, value}] = :dets.lookup(raft_log.table_name, :coordinator_checkpoint)
    value
  end

  defp coherent_checkpoint_state(coordinator) do
    state = :sys.get_state(coordinator)
    cp = checkpoint(state)
    if cp.last_durable_txn_id == state.last_durable_txn_id, do: {:ok, state}, else: :retry
  end

  defp assert_allocation_and_checkpoint!(state, authority) do
    raft_log = Bedrock.Raft.log(state.raft)
    first = Bedrock.Raft.Log.initial_transaction_id(raft_log)
    last = Bedrock.Raft.Log.newest_safe_transaction_id(raft_log)
    limit = elem(last, 1) - elem(first, 1)

    allocations =
      raft_log
      |> Bedrock.Raft.Log.transactions_from(first, last, limit)
      |> Enum.filter(fn {_id, command} -> match?({:begin_recovery, _}, command) end)

    matching =
      Enum.filter(allocations, fn {_id, {:begin_recovery, allocation}} ->
        allocation.request_id == authority.recovery_id
      end)

    assert [{allocation_id, {:begin_recovery, allocation}}] = matching
    assert allocation == state.last_allocation
    assert allocation.generation == authority.generation

    cp = checkpoint(state)
    assert cp.format_version == 1
    assert cp.cluster_id == state.cluster_id
    assert state.generation_floor == authority.generation
    assert cp.generation_floor == state.generation_floor
    assert cp.last_allocation == allocation
    assert allocation_id <= cp.last_durable_txn_id
    assert cp.last_durable_txn_id == state.last_durable_txn_id

    %{transaction_id: allocation_id, allocation: allocation, checkpoint_cursor: cp.last_durable_txn_id}
  end

  defp retained_history(workers, expected, version) do
    logs = Enum.filter(workers, &(&1.kind == :log))

    {source, transactions} =
      case normal_log_history(logs) do
        {:ok, transactions} -> {:normal_pull, transactions}
        :unavailable -> {:wal, Enum.flat_map(logs, &wal_transactions/1)}
      end

    versions =
      transactions
      |> Enum.map(fn bytes -> {Transaction.commit_version!(bytes), bytes} end)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    %{
      source: source,
      version: version,
      occurrences: Enum.count(transactions, &(&1 == expected)),
      conflicting_versions:
        for(
          {commit_version, binaries} <- versions,
          length(Enum.uniq(binaries)) > 1,
          do: commit_version
        )
    }
  end

  defp normal_log_history(logs) do
    Enum.reduce_while(logs, {:ok, []}, fn worker, {:ok, acc} ->
      state = :sys.get_state(worker.pid, 1_000)

      case Log.pull(worker.pid, state.available_after,
             last_version: state.last_version,
             limit: 100_000,
             timeout_in_ms: 1_000
           ) do
        {:ok, transactions} -> {:cont, {:ok, transactions ++ acc}}
        _ -> {:halt, :unavailable}
      end
    end)
  end

  defp wal_transactions(worker) do
    worker.path
    |> File.ls!()
    |> Enum.filter(&String.starts_with?(&1, "wal_"))
    |> Enum.sort()
    |> Enum.flat_map(fn name ->
      worker.path
      |> Path.join(name)
      |> TransactionStreams.from_file!()
      |> Enum.to_list()
      |> Enum.reject(&match?({:error, _}, &1))
    end)
  end

  defp stable_snapshot(coordinator, workers, backend) do
    state = :sys.get_state(coordinator)
    bootstrap = read_bootstrap!(backend)

    %{
      coordinator:
        Map.take(state, [
          :epoch,
          :config,
          :prior_core_state,
          :transaction_system_layout,
          :publication_sequences,
          :generation_floor,
          :last_allocation
        ]),
      link_layout: Cluster.transaction_system_layout!(),
      bootstrap: {bootstrap.bytes, bootstrap.version_token},
      workers: Map.new(workers, &{&1.id, protected_worker_snapshot(&1)})
    }
  end

  defp protected_worker_snapshot(worker) do
    state = :sys.get_state(worker.pid, 1_000)

    %{
      mode: state.mode,
      authority: external_authority(state.recovery_authority),
      control: File.read!(RecoveryControl.path(worker.path)),
      last_version: Map.get(state, :last_version),
      replay_operation: Map.get(state, :replay_operation),
      pending_pushes: Map.get(state, :pending_pushes),
      wal: wal_bytes(worker),
      durable_data: durable_data_bytes(worker)
    }
  end

  defp wal_bytes(%{kind: :log, path: path}) do
    path
    |> File.ls!()
    |> Enum.filter(&String.starts_with?(&1, "wal_"))
    |> Enum.sort()
    |> Map.new(&{&1, File.read!(Path.join(path, &1))})
  end

  defp wal_bytes(_), do: nil

  defp durable_data_bytes(%{kind: :materializer, path: path}) do
    ["data", "idx"]
    |> Enum.filter(&File.regular?(Path.join(path, &1)))
    |> Map.new(&{&1, File.read!(Path.join(path, &1))})
  end

  defp durable_data_bytes(_), do: nil

  defp reject_stale_storage_work!(workers, old_authority, foreign_authority) do
    log = Enum.find(workers, &(&1.kind == :log))
    materializer = Enum.find(workers, &(&1.kind == :materializer))
    before = %{log.id => protected_worker_snapshot(log), materializer.id => protected_worker_snapshot(materializer)}

    assert {:error, :newer_epoch_exists} = Log.lock_for_recovery(log.pid, old_authority)
    assert {:error, :not_lock_owner} = Log.lock_for_recovery(log.pid, foreign_authority)
    assert {:error, :newer_epoch_exists} = Materializer.lock_for_recovery(materializer.pid, old_authority)
    assert {:error, :not_lock_owner} = Materializer.lock_for_recovery(materializer.pid, foreign_authority)

    state = :sys.get_state(log.pid)
    stale_version = Version.increment(state.last_version)

    stale_transaction =
      Transaction.encode(%{
        mutations: [{:set, "epoch/stale-push", "must-not-appear"}],
        shard_index: [{0, 1}],
        commit_version: stale_version
      })

    assert {:error, :not_lock_owner} =
             Log.push(log.pid, old_authority, stale_transaction, state.last_version,
               known_committed_version: state.last_version
             )

    assert {:error, :not_lock_owner} =
             Log.push(log.pid, foreign_authority, stale_transaction, state.last_version,
               known_committed_version: state.last_version
             )

    after_calls = %{
      log.id => protected_worker_snapshot(log),
      materializer.id => protected_worker_snapshot(materializer)
    }

    assert after_calls == before

    assert nil ==
             Repo.transact(fn -> Repo.get("epoch/stale-push") end,
               retry_limit: 0,
               timeout_in_ms: 1_000
             )

    %{
      lower_log_lock: :newer_epoch_exists,
      foreign_log_lock: :not_lock_owner,
      lower_materializer_lock: :newer_epoch_exists,
      foreign_materializer_lock: :not_lock_owner,
      lower_log_push: :not_lock_owner,
      foreign_log_push: :not_lock_owner,
      stale_version: stale_version
    }
  end

  defp assert_stable_snapshot!(before, coordinator, workers, backend) do
    assert stable_snapshot(coordinator, workers, backend) == before
  end

  defp publication_identity(state) do
    case Map.get(state, :bootstrap_reservation) do
      %{recovery_id: id} ->
        sequences = Map.get(state, :publication_sequences, %{layout: 1})
        {:attributed, id, sequences.layout}

      _ ->
        :legacy
    end
  end

  defp deliver_old_publication(coordinator, old_director, layout, old_state, {:attributed, id, sequence}) do
    GenServer.cast(
      coordinator,
      {:notify_transaction_system_layout, {old_director, layout.epoch, sequence}, id, layout,
       old_state.prior_core_state}
    )

    :sys.get_state(coordinator)
  end

  defp deliver_old_publication(coordinator, _old_director, layout, old_state, :legacy) do
    GenServer.cast(coordinator, {:notify_transaction_system_layout, layout, old_state.prior_core_state})
    :sys.get_state(coordinator)
  end

  defp external_authority(%{generation: generation, recovery_id: recovery_id}),
    do: %{generation: generation, recovery_id: recovery_id}

  defp external_authority(_), do: nil

  defp legal_tail?({<<0::little-unsigned-64>>, nil}), do: true
  defp legal_tail?({<<1::little-unsigned-64>>, "ambiguous"}), do: true
  defp legal_tail?(_), do: false

  defp reservation_authority(%{generation: generation, recovery_id: recovery_id}, generation),
    do: %{generation: generation, recovery_id: recovery_id}

  defp reservation_authority(_, _), do: nil

  defp eventually(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    eventually_until(fun, deadline)
  end

  defp eventually_until(fun, deadline) do
    case fun.() do
      {:ok, _} = result ->
        result

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(25)
          eventually_until(fun, deadline)
        end
    end
  end
end
