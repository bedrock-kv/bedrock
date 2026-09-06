defmodule Bedrock.ControlPlane.Director.PublicationProtocolTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Director.Publication
  alias Bedrock.ControlPlane.Director.Server
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.Raft.Log.InMemoryLog

  @moduletag capture_log: true

  defmodule Actor do
    @moduledoc false

    # Enter the production Server loop with recovery already completed. Publication
    # starts inside the actor, preserving the real sender identity and timer owner.
    def start(state, core_state) do
      state = Publication.start(state, core_state)
      :proc_lib.init_ack({:ok, self()})
      :gen_server.enter_loop(Server, [], state)
    end
  end

  setup do
    state = %State{
      state: :running,
      epoch: 8,
      coordinator: self(),
      publication_sequence: 4,
      bootstrap_reservation: %{recovery_id: "publication-8"},
      transaction_system_layout: %{epoch: 8, logs: %{"log-8" => [0, 1]}}
    }

    core_state = %{logs: %{"log-8" => [0, 1]}, marker: "immutable completed recovery"}
    {:ok, director} = :proc_lib.start(Actor, :start, [state, core_state])
    monitor = Process.monitor(director)

    on_exit(fn ->
      if Process.alive?(director), do: Process.exit(director, :kill)
    end)

    %{director: director, monitor: monitor, layout: state.transaction_system_layout, core_state: core_state}
  end

  test "retries send exactly the same completed payload three times, then the Director dies", fixture do
    %{director: director, monitor: monitor, layout: layout, core_state: core_state} = fixture
    expected = {:notify_transaction_system_layout, {director, 8, 5}, "publication-8", layout, core_state}
    assert_receive {:"$gen_cast", ^expected}

    # Later live state changes must not change the already pending publication.
    :sys.replace_state(director, fn state ->
      %{state | transaction_system_layout: %{epoch: 999}, prior_core_state: %{changed: true}}
    end)

    for sends <- [2, 3] do
      deliver_retry(director)
      assert_receive {:"$gen_cast", ^expected}
      assert :sys.get_state(director).pending_publication.sends == sends
    end

    deliver_retry(director)

    assert_receive {:DOWN, ^monitor, :process, ^director,
                    {:shutdown, {:recovery_publication_failed, :publication_ack_timeout}}}

    refute_received {:"$gen_cast", {:notify_transaction_system_layout, _, _, _, _}}
  end

  test "matching acknowledgement cancels its timer and makes queued retries inert", %{director: director} do
    assert_receive {:"$gen_cast", {:notify_transaction_system_layout, {^director, 8, 5}, "publication-8", _, _}}
    pending = :sys.get_state(director).pending_publication
    GenServer.cast(director, {:publication_ack, self(), pending.id, pending.sequence})
    assert :sys.get_state(director).pending_publication == nil
    assert Process.read_timer(pending.timer) == false

    send(director, {:publication_retry, pending.id, pending.sequence})
    assert :sys.get_state(director).pending_publication == nil
    assert Process.alive?(director)
    refute_received {:"$gen_cast", {:notify_transaction_system_layout, _, _, _, _}}
  end

  test "wrong coordinator, publication identity, and sequence cannot acknowledge pending work", %{
    director: director
  } do
    assert_receive {:"$gen_cast", {:notify_transaction_system_layout, {^director, 8, 5}, "publication-8", _, _}}
    pending = :sys.get_state(director).pending_publication

    for ack <- [
          {:publication_ack, director, pending.id, pending.sequence},
          {:publication_ack, self(), "stale-publication", pending.sequence},
          {:publication_ack, self(), pending.id, pending.sequence - 1}
        ] do
      GenServer.cast(director, ack)
      assert :sys.get_state(director).pending_publication == pending
    end

    deliver_retry(director)
    assert_receive {:"$gen_cast", {:notify_transaction_system_layout, {^director, 8, 5}, "publication-8", _, _}}
    assert :sys.get_state(director).pending_publication.sends == 2
  end

  test "a retry delivers a first notification that the Coordinator never received", fixture do
    %{director: director, layout: layout, core_state: core_state} = fixture
    assert_receive {:"$gen_cast", {:notify_transaction_system_layout, _, _, _, _}}
    deliver_retry(director)

    assert_receive {:"$gen_cast", retry = {:notify_transaction_system_layout, _, _, _, _}}
    coordinator = coordinator_state(director, layout, core_state, false)
    assert {:noreply, accepted} = Bedrock.ControlPlane.Coordinator.Server.handle_cast(retry, coordinator)
    assert accepted.transaction_system_layout == layout
    assert accepted.prior_core_state == core_state
    assert :sys.get_state(director).pending_publication == nil
  end

  test "a retry after acknowledgement loss receives the Coordinator's duplicate acknowledgement", fixture do
    %{director: director, layout: layout, core_state: core_state} = fixture
    assert_receive {:"$gen_cast", {:notify_transaction_system_layout, _, _, _, _}}
    coordinator = coordinator_state(director, layout, core_state, true)
    deliver_retry(director)

    assert_receive {:"$gen_cast", retry = {:notify_transaction_system_layout, _, _, _, _}}
    assert {:noreply, ^coordinator} = Bedrock.ControlPlane.Coordinator.Server.handle_cast(retry, coordinator)
    assert :sys.get_state(director).pending_publication == nil
  end

  # Drive the existing production timer message after cancelling its scheduled
  # delivery. State barriers establish ordering; no wall-clock sleeps are needed.
  defp deliver_retry(director) do
    pending = :sys.get_state(director).pending_publication
    assert is_integer(Process.cancel_timer(pending.timer))
    send(director, {:publication_retry, pending.id, pending.sequence})
  end

  defp coordinator_state(director, layout, core_state, accepted?) do
    raft =
      node()
      |> Bedrock.Raft.new(
        [],
        InMemoryLog.new(:tuple),
        RaftAdapter
      )
      |> Bedrock.Raft.handle_event(:election, :timer)

    %Bedrock.ControlPlane.Coordinator.State{
      director: director,
      epoch: 8,
      bootstrap_reservation: %{generation: 8, recovery_id: "publication-8"},
      raft: raft,
      leader_node: node(),
      my_node: node(),
      raft_term: 1,
      director_raft_term: 1,
      publication_sequences: %{config: 0, layout: if(accepted?, do: 5, else: 0)},
      transaction_system_layout: if(accepted?, do: layout),
      prior_core_state: if(accepted?, do: core_state)
    }
  end
end

defmodule Bedrock.ControlPlane.Director.PublicationFailureActorTest do
  use ExUnit.Case, async: false

  alias Bedrock.ClusterBootstrap.Publication, as: BootstrapPublication
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Raft.Log
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :distributed
  @moduletag :tmp_dir
  @moduletag capture_log: true
  @moduletag timeout: 30_000

  defmodule Cluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "publication_failure_actor"
  end

  defmodule BeforeCluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "publication_before_cas_actor"
  end

  defmodule BeforeRepo do
    use Bedrock.Repo, cluster: BeforeCluster
  end

  defmodule AfterCluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "publication_after_cas_actor"
  end

  defmodule AfterRepo do
    use Bedrock.Repo, cluster: AfterCluster
  end

  defmodule FailingPublicationStorage do
    @moduledoc false
    defdelegate put(config, key, data, opts), to: LocalFilesystem
    defdelegate get(config, key), to: LocalFilesystem
    defdelegate delete(config, key), to: LocalFilesystem
    defdelegate list(config, prefix, opts), to: LocalFilesystem
    defdelegate put_if_not_exists(config, key, data, opts), to: LocalFilesystem
    defdelegate get_with_version(config, key), to: LocalFilesystem

    def put_if_version_matches(config, key, version, data, opts) do
      case ClusterBootstrap.read(IO.iodata_to_binary(data)) do
        {:ok, %{publication_id: id, recovery_id: id} = bootstrap} when is_binary(id) and byte_size(id) > 0 ->
          owner = Keyword.fetch!(config, :test_owner)

          mode = fault_mode(config)

          case mode do
            :reject ->
              gate(owner, {:final_publication_blocked, self(), key, bootstrap})
              {:error, :access_denied}

            :before_cas ->
              gate(owner, {:publication_cut, self(), key, bootstrap, mode})
              LocalFilesystem.put_if_version_matches(config, key, version, data, opts)

            :after_cas ->
              :ok = LocalFilesystem.put_if_version_matches(config, key, version, data, opts)
              gate(owner, {:publication_cut, self(), key, bootstrap, mode})
              :ok

            :none ->
              result = LocalFilesystem.put_if_version_matches(config, key, version, data, opts)
              send(owner, {:publication_result, self(), key, bootstrap, result})
              result
          end

        _ ->
          LocalFilesystem.put_if_version_matches(config, key, version, data, opts)
      end
    end

    defp fault_mode(config) do
      case Keyword.get(config, :fault_control) do
        nil -> :reject
        control -> Agent.get_and_update(control, fn mode -> {mode, :none} end)
      end
    end

    defp gate(owner, message) do
      monitor = Process.monitor(owner)
      send(owner, message)

      # Owner death/timeout releases the boundary even if an assertion fails.
      receive do
        :reject_publication -> :ok
        {:DOWN, ^monitor, :process, ^owner, _} -> :ok
      after
        10_000 -> :ok
      end

      Process.demonitor(monitor, [:flush])
    end
  end

  test "a real persistence publication failure terminates the recovering Director", %{tmp_dir: root} do
    assert Node.alive?()
    previous = Application.get_env(:bedrock, Cluster)
    previous_storage = Application.get_env(:bedrock, ObjectStorage)
    backend = {FailingPublicationStorage, root: Path.join(root, "objects"), test_owner: self()}
    Application.put_env(:bedrock, ObjectStorage, backend: backend)

    Application.put_env(:bedrock, Cluster,
      capabilities: [:coordination, :log, :materializer],
      durability_mode: :relaxed,
      path_to_descriptor: Path.join(root, "descriptor"),
      object_storage: backend,
      coordinator: [path: root],
      materializer: [path: root, object_storage: backend],
      log: [path: root, object_storage: backend]
    )

    on_exit(fn ->
      restore(Cluster, previous)
      restore(ObjectStorage, previous_storage)
    end)

    start_supervised!({Cluster, []})
    assert_receive {:final_publication_blocked, director, key, candidate}, 15_000
    monitor = Process.monitor(director)
    coordinator = Process.whereis(Cluster.otp_name(:coordinator))
    assert :sys.get_state(coordinator).director == director
    assert candidate.logs != []
    assert candidate.epoch == candidate.recovery_generation

    send(director, :reject_publication)

    reason = {:shutdown, {:recovery_publication_failed, {:bootstrap_publication_failed, :publication_mismatch}}}
    assert_receive {:DOWN, ^monitor, :process, ^director, ^reason}, 5_000

    assert {:ok, bytes} = LocalFilesystem.get(elem(backend, 1), key)
    assert {:ok, retained} = ClusterBootstrap.read(bytes)
    assert Map.get(retained, :publication_id) != candidate.publication_id
  end

  for cut <- [:before_cas, :after_cas] do
    test "real Director crash at #{cut} preserves data and publishes a greater successor generation", %{tmp_dir: root} do
      crash_cut(root, unquote(cut))
    end
  end

  defp crash_cut(root, cut) do
    assert Node.alive?()
    {cluster, repo} = if cut == :before_cas, do: {BeforeCluster, BeforeRepo}, else: {AfterCluster, AfterRepo}
    control = start_supervised!({Agent, fn -> :none end})
    backend = {FailingPublicationStorage, root: Path.join(root, "objects"), test_owner: self(), fault_control: control}
    configure_cluster(root, backend, cluster)
    start_supervised!({cluster, []})
    assert :ok = repo.transact(fn -> repo.put("publication/acknowledged", "retained-value") end, timeout_in_ms: 15_000)
    assert "retained-value" = repo.transact(fn -> repo.get("publication/acknowledged") end, timeout_in_ms: 15_000)
    coordinator = Process.whereis(cluster.otp_name(:coordinator))
    initial = :sys.get_state(coordinator)
    assert_receive {:publication_result, first, key, initial_bootstrap, :ok}
    assert first == initial.director
    Agent.update(control, fn _ -> cut end)
    first_down = Process.monitor(first)
    Process.exit(first, :kill)
    assert_receive {:DOWN, ^first_down, :process, ^first, :killed}

    assert_receive {:publication_cut, crashed, ^key, candidate, ^cut}, 15_000
    at_cut = :sys.get_state(coordinator)
    assert at_cut.director == crashed
    assert at_cut.epoch == candidate.epoch
    assert candidate.epoch > initial.epoch
    assert at_cut.raft_term == initial.raft_term
    assert at_cut.transaction_system_layout == nil
    assert {:ok, stored_at_cut} = BootstrapPublication.read({LocalFilesystem, elem(backend, 1)}, key)

    expected_prior = if cut == :after_cas, do: candidate, else: initial_bootstrap
    assert stored_at_cut.bootstrap.epoch == expected_prior.epoch
    assert stored_at_cut.bootstrap.publication_id == expected_prior.publication_id
    assert stored_at_cut.bootstrap.logs == expected_prior.logs
    assert stored_at_cut.bootstrap.recovery_generation == candidate.epoch

    crashed_down = Process.monitor(crashed)
    Process.exit(crashed, :kill)
    assert_receive {:DOWN, ^crashed_down, :process, ^crashed, :killed}
    assert_receive {:publication_result, successor, ^key, completed, :ok}, 15_000
    assert successor != crashed
    assert completed.epoch > candidate.epoch
    assert "retained-value" = repo.transact(fn -> repo.get("publication/acknowledged") end, timeout_in_ms: 15_000)
    recovered = :sys.get_state(coordinator)
    assert recovered.director == successor
    assert recovered.raft_term == initial.raft_term
    assert recovered.epoch == completed.epoch
    assert recovered.transaction_system_layout.epoch == completed.epoch
    assert recovered.bootstrap_reservation.prior_bootstrap == stored_at_cut.bootstrap
    assert recovered.bootstrap_reservation.generation == completed.epoch
    assert {:ok, durable} = BootstrapPublication.read({LocalFilesystem, elem(backend, 1)}, key)
    assert durable.bootstrap == completed
    assert durable.bootstrap.publication_id == recovered.bootstrap_reservation.recovery_id

    log = Bedrock.Raft.log(recovered.raft)

    allocations =
      for {_id, {:begin_recovery, allocation}} <- Log.transactions_to(log, :newest_safe), do: allocation

    assert Enum.map(allocations, & &1.generation) == [initial.epoch, candidate.epoch, completed.epoch]
    assert length(Enum.uniq_by(allocations, & &1.request_id)) == 3
    assert [{:coordinator_checkpoint, checkpoint}] = :dets.lookup(log.table_name, :coordinator_checkpoint)
    assert checkpoint.generation_floor == completed.epoch
    assert checkpoint.last_allocation == List.last(allocations)
    refute_received {:publication_result, ^crashed, _, _, _}
    refute_received {:publication_cut, ^crashed, _, _, _}

    File.write!(
      Path.join(root, "publication-cut.term"),
      :erlang.term_to_binary(%{
        cut: cut,
        initial: initial_bootstrap,
        candidate: candidate,
        stored_at_cut: stored_at_cut.bootstrap,
        completed: completed,
        allocations: allocations,
        checkpoint: checkpoint
      })
    )
  end

  defp configure_cluster(root, backend, cluster) do
    previous = Application.get_env(:bedrock, cluster)
    previous_storage = Application.get_env(:bedrock, ObjectStorage)
    Application.put_env(:bedrock, ObjectStorage, backend: backend)

    Application.put_env(:bedrock, cluster,
      capabilities: [:coordination, :log, :materializer],
      durability_mode: :relaxed,
      path_to_descriptor: Path.join(root, "descriptor"),
      object_storage: backend,
      coordinator: [path: root],
      materializer: [path: root, object_storage: backend],
      log: [path: root, object_storage: backend]
    )

    on_exit(fn ->
      restore(cluster, previous)
      restore(ObjectStorage, previous_storage)
    end)
  end

  defp restore(key, nil), do: Application.delete_env(:bedrock, key)
  defp restore(key, value), do: Application.put_env(:bedrock, key, value)
end
