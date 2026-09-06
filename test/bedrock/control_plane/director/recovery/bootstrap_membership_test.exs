defmodule Bedrock.ControlPlane.Director.Recovery.BootstrapMembershipTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.ControlPlane.Director.Recovery.LogReplayPhase
  alias Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase
  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase
  alias Bedrock.ControlPlane.Distributor.Lock
  alias Bedrock.ControlPlane.Distributor.Server, as: Distributor
  alias Bedrock.ControlPlane.Distributor.State, as: DistributorState
  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.Server, as: Olivine
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.ClusterBootstrap
  alias Bedrock.SystemKeys.Values

  def otp_name_for_worker(id), do: :"bootstrap_membership_#{id}"
  def otp_name(:foreman), do: :bootstrap_membership_foreman
  def node_config, do: [object_storage: Process.get({__MODULE__, :backend})]

  defmodule StaleBootstrapToken do
    @moduledoc false
    def get_with_version(opts, key) do
      with {:ok, data, _token} <- LocalFilesystem.get_with_version(opts, key) do
        {:ok, data, "sha256:stale-token"}
      end
    end

    defdelegate put_if_version_matches(opts, key, token, data, write_opts), to: LocalFilesystem
  end

  defmodule LogDiscovery do
    @moduledoc false
    use GenServer

    def start_link(shard), do: GenServer.start_link(__MODULE__, shard)
    @impl true
    def init(shard), do: {:ok, shard}
    @impl true
    def handle_call({:get_shard_server, 0}, _from, shard), do: {:reply, {:ok, shard}, shard}
  end

  for {previous_member, publication} <- [
        {:displaced, :before_system_commit},
        {:snapshot, :before_system_commit},
        {:legacy, :before_system_commit},
        {:dead, :before_add},
        {:dead, :after_add},
        {:dead, :before_system_commit},
        {:dead, :failed_bootstrap_cas},
        {:dead, :after_bootstrap}
      ] do
    @tag :tmp_dir
    test "#{previous_member} cache with crash #{publication} preserves authoritative bootstrap history", %{
      tmp_dir: path
    } do
      backend = ObjectStorage.backend(LocalFilesystem, root: Path.join(path, "objects"))
      {:ok, shard} = ShardServer.start_link(shard_id: 0, cluster: __MODULE__, object_storage: backend)
      {:ok, log} = start_supervised({LogDiscovery, shard})
      on_exit(fn -> if Process.alive?(shard), do: GenServer.stop(shard) end)
      w1 = "old_#{System.unique_integer([:positive])}"
      w2 = "replacement_#{System.unique_integer([:positive])}"
      node_string = Atom.to_string(node())
      layout = %{"m" => {7, ""}, <<0xFF>> => {9, "m"}, Bedrock.end_of_keyspace() => {0, <<0xFF>>}}
      v100 = Version.from_integer(100)
      v200 = Version.from_integer(200)
      v300 = Version.from_integer(300)

      seed =
        Enum.map(layout, fn {end_key, {tag, start_key}} ->
          {:set, SystemKeys.shard_key(end_key), Values.encode_shard_key_entry(tag, start_key)}
        end) ++ [{:set, SystemKeys.materializer_key(0, w1), Values.encode_materializer_node(node_string)}]

      push(shard, 100, seed)
      ShardServer.flush(shard, v100)
      assert_receive {:durable, ^shard, 0, ^v100}, 5_000

      old = start_materializer(path, w1)
      unlock(old, 1, log, v100)
      assert {:ok, _} = Materializer.get(old, SystemKeys.materializer_key(0, w1), v100, wait_ms: 5_000)
      checkpoint = Path.join(path, "checkpoint")

      if unquote(previous_member) == :snapshot do
        :sys.replace_state(old, fn state ->
          state = %{
            state
            | index_manager: %{state.index_manager | window_lag_time_μs: 0},
              known_committed_version: v100
          }

          {:ok, state} = Logic.advance_window(state)
          state
        end)

        File.mkdir_p!(checkpoint)
        for name <- ["data", "idx"], do: File.cp!(Path.join([path, w1, name]), Path.join(checkpoint, name))
      end

      replacement = start_materializer(path, w2)
      unlock(replacement, 1, log, v100)

      {lock, _} = Lock.take(nil, nil)
      counter = :counters.new(1, [])
      :counters.put(counter, 1, 100)

      deps = %{
        epoch: 1,
        proxies: [:proxy],
        next_read_version_fn: fn -> {:ok, Version.from_integer(:counters.get(counter, 1))} end,
        get_fn: fn _key, _version -> {:ok, lock.my_owner} end,
        commit_fn: fn _proxy, 1, encoded, _opts ->
          :counters.add(counter, 1, 100)
          version = :counters.get(counter, 1)
          push(shard, version, Transaction.mutations!(encoded))
          {:ok, Version.from_integer(version), 0}
        end
      }

      state = %DistributorState{
        cluster: __MODULE__,
        epoch: 1,
        director: self(),
        director_monitor: make_ref(),
        lock: lock,
        deps: deps,
        placeholder: self(),
        snapshot: %{shard_layout: layout, materializer_refs: %{0 => %{w1 => node_string}}}
      }

      assert {:noreply, state} =
               Distributor.handle_info({:recruitment_complete, 0, {:ok, replacement, node(), w2}}, state)

      assert state.snapshot.materializer_refs[0] |> Map.keys() |> Enum.sort() == Enum.sort([w1, w2])
      assert {:ok, _} = Materializer.get(replacement, SystemKeys.materializer_key(0, w2), v200, wait_ms: 5_000)

      if unquote(previous_member) == :dead, do: stop_supervised!(w1)
      monitor = make_ref()
      state = %{state | assignment_monitors: Map.put(state.assignment_monitors, monitor, {0, w1})}
      assert {:noreply, retired} = Distributor.handle_info({:DOWN, monitor, :process, old, :killed}, state)
      assert retired.snapshot.materializer_refs[0] == %{w2 => node_string}

      assert {:error, :not_found} =
               Materializer.get(replacement, SystemKeys.materializer_key(0, w1), v300, wait_ms: 5_000)

      # Recovery creates a new log's ShardServer: no inherited durable
      # watermark or buffer. Only the retained WAL suffix is replayed;
      # rebuilding the layout requires independently reading the old chunk.
      {:ok, new_shard} = ShardServer.start_link(shard_id: 0, cluster: __MODULE__, object_storage: backend)
      on_exit(fn -> if Process.alive?(new_shard), do: GenServer.stop(new_shard) end)
      assert ShardServer.durable_version(new_shard) == nil

      for {version, encoded} <- Enum.reverse(:sys.get_state(shard).buffer) do
        ShardServer.push(new_shard, version, encoded, v300)
      end

      {:ok, new_log} = start_supervised(Supervisor.child_spec({LogDiscovery, new_shard}, id: :new_log))

      # Epoch 2 still names w1 out of band, despite the committed family
      # having replaced it. An arbitrary tag-0 claim is not recovery authority.
      assert {:ok, ^replacement, info} = Materializer.lock_for_recovery(replacement, 2)

      attempt =
        recovery_attempt(%{
          cluster: __MODULE__,
          epoch: 2,
          logs: %{"log" => []},
          version_vector: {v100, v300},
          materializer_recovery_info_by_id: %{w2 => info},
          transaction_services: %{
            w2 => %{kind: :materializer, status: {:up, replacement}},
            "log" => %{kind: :log, status: {:up, new_log}}
          }
        })

      attempt =
        if unquote(previous_member) in [:displaced, :legacy] do
          {:ok, ^old, old_info} = Materializer.lock_for_recovery(old, 2)

          %{
            attempt
            | materializer_recovery_info_by_id: Map.put(attempt.materializer_recovery_info_by_id, w1, old_info),
              transaction_services:
                Map.put(attempt.transaction_services, w1, %{kind: :materializer, status: {:up, old}})
          }
        else
          attempt
        end

      context =
        %{
          prior_core_state: %{
            logs: %{"prior-log" => []},
            system_materializers: if(unquote(previous_member) == :legacy, do: %{}, else: %{w1 => node_string})
          },
          node_capabilities: %{materializer: [node()]},
          create_worker_fn: fn _foreman, id, :materializer, opts ->
            assert opts[:params] == %{"shard_id" => 0}

            if unquote(previous_member) == :snapshot do
              File.mkdir_p!(Path.join(path, id))
              for name <- ["data", "idx"], do: File.cp!(Path.join(checkpoint, name), Path.join([path, id, name]))
            end

            pid = start_materializer(path, id)

            if unquote(previous_member) == :snapshot do
              assert {:ok, %{current_version: ^v100}} = Materializer.info(pid, [:current_version])
            end

            {:ok, otp_name_for_worker(id)}
          end,
          catchup_poll_interval_ms: 5,
          catchup_timeout_ms: 5_000
        }
        |> recovery_context()
        |> Map.delete(:read_prior_refs_fn)

      assert {recovered, CommitProxyStartupPhase} = MaterializerBootstrapPhase.execute(attempt, context)
      assert recovered.shard_layout == layout
      refute recovered.seeded_layout?
      assert recovered.prior_materializer_refs[0] == %{w2 => node_string}
      [{reconstructed_id, ^node_string}] = Map.to_list(recovered.shard_materializers[0])
      refute reconstructed_id in [w1, w2]
      assert %{status: {:up, reconstructed}} = recovered.transaction_services[reconstructed_id]
      assert {:ok, _} = Materializer.get(reconstructed, SystemKeys.shard_key(Bedrock.end_of_keyspace()), v300)

      verify_publication_crash(unquote(publication), backend, context, recovered, %{
        old_log: log,
        old_shard: shard,
        new_log: new_log,
        new_shard: new_shard,
        w1: w1,
        w2: w2,
        reconstructed_id: reconstructed_id,
        reconstructed: reconstructed,
        layout: layout
      })
    end
  end

  defp verify_publication_crash(boundary, {_, opts} = backend, context, recovered, fixture) do
    original = %{
      cluster_id: "membership-test",
      epoch: 1,
      logs: [%{id: "prior-log", otp_ref: nil, shard_tags: []}],
      system_materializers: [%{id: fixture.w1, node: Atom.to_string(node())}],
      coordinators: [%{node: Atom.to_string(node())}],
      parameters: context.cluster_config.parameters,
      policies: context.cluster_config.policies
    }

    assert :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(original))

    Process.put(
      {__MODULE__, :backend},
      if(boundary == :failed_bootstrap_cas, do: {StaleBootstrapToken, opts}, else: backend)
    )

    attempt = %{recovered | proxies: [self()], transaction_system_layout: %{logs: %{"epoch2-log" => []}}}
    v400 = Version.from_integer(400)

    commit_context =
      Map.put(context, :commit_transaction_fn, fn _, 2, encoded ->
        # The recovery transaction is not yet a published epoch: its push
        # carries the previous KCV, not a fictitious commit confirmation.
        txn = Transaction.encode(%{mutations: Enum.to_list(Transaction.mutations!(encoded)), commit_version: v400})
        ShardServer.push(fixture.new_shard, v400, txn, Version.from_integer(300))
        {:ok, v400, 0}
      end)

    publish_at_boundary(boundary, attempt, commit_context, fixture, v400)

    assert {:ok, binary} = ObjectStorage.get(backend, "bootstrap")
    assert {:ok, bootstrap} = ClusterBootstrap.read(binary)
    core = CoreState.from_bootstrap(bootstrap)
    expected_log = if boundary == :after_bootstrap, do: "epoch2-log", else: "prior-log"
    assert Map.keys(core.logs) == [expected_log]

    rv = recovery_version_at_boundary(boundary)

    # Crash all read caches. The next attempt must recover from exactly the
    # LOG identities still named by the actual bootstrap object; a committed
    # membership write before failed CAS does not change that identity.
    for id <- [fixture.w1, fixture.w2, fixture.reconstructed_id], do: stop_supervised(id)
    {:ok, replayed_shard} = ShardServer.start_link(shard_id: 0, cluster: __MODULE__, object_storage: backend)
    on_exit(fn -> if Process.alive?(replayed_shard), do: GenServer.stop(replayed_shard) end)
    assert ShardServer.durable_version(replayed_shard) == nil
    {:ok, replayed_log} = start_supervised(Supervisor.child_spec({LogDiscovery, replayed_shard}, id: :replayed_log))
    source_logs = %{"prior-log" => fixture.old_log, "epoch2-log" => fixture.new_log}

    next_attempt =
      recovery_attempt(%{
        cluster: __MODULE__,
        epoch: 3,
        logs: %{"epoch3-log" => []},
        old_log_ids_to_copy: Map.keys(core.logs),
        version_vector: {Version.from_integer(100), rv},
        service_pids: Map.put(source_logs, "epoch3-log", replayed_log),
        transaction_services: %{"epoch3-log" => %{kind: :log, status: {:up, replayed_log}}}
      })

    replay_context =
      Map.put(context, :copy_log_data_fn, fn "epoch3-log", sources, after_version, through_version, _services ->
        assert sources == [Map.fetch!(source_logs, expected_log)]
        {:ok, source_shard} = GenServer.call(hd(sources), {:get_shard_server, 0})

        for {version, encoded} <- Enum.reverse(:sys.get_state(source_shard).buffer),
            version > after_version and version <= through_version do
          ShardServer.push(replayed_shard, version, encoded, through_version)
        end

        {:ok, replayed_log}
      end)

    assert {next_attempt, Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase} =
             LogReplayPhase.execute(next_attempt, replay_context)

    next_context = Map.put(context, :prior_core_state, core)
    assert {next, CommitProxyStartupPhase} = MaterializerBootstrapPhase.execute(next_attempt, next_context)
    assert next.shard_layout == fixture.layout
    refute next.seeded_layout?

    expected_members = members_at_boundary(boundary, fixture)

    assert next.prior_materializer_refs[0] == expected_members
    [{new_id, _}] = Map.to_list(next.shard_materializers[0])
    refute new_id in [fixture.w1, fixture.w2, fixture.reconstructed_id]
  end

  defp publish_at_boundary(boundary, attempt, commit_context, fixture, v400) do
    case boundary do
      :failed_bootstrap_cas ->
        assert {_, {:stalled, {:recovery_system_failed, :bootstrap_version_mismatch}}} =
                 PersistencePhase.execute(attempt, commit_context)

        assert Process.alive?(fixture.reconstructed)

        assert {:ok, _} =
                 Materializer.get(fixture.reconstructed, SystemKeys.materializer_key(0, fixture.reconstructed_id), v400,
                   wait_ms: 5_000
                 )

      :after_bootstrap ->
        assert {_, :completed} = PersistencePhase.execute(attempt, commit_context)

      _before_publication ->
        :ok
    end
  end

  defp recovery_version_at_boundary(boundary) do
    version =
      case boundary do
        :before_add -> 100
        :after_add -> 200
        :after_bootstrap -> 400
        _ -> 300
      end

    Version.from_integer(version)
  end

  defp members_at_boundary(boundary, fixture) do
    case boundary do
      :before_add -> %{fixture.w1 => Atom.to_string(node())}
      :after_add -> %{fixture.w1 => Atom.to_string(node()), fixture.w2 => Atom.to_string(node())}
      :after_bootstrap -> %{fixture.w2 => Atom.to_string(node()), fixture.reconstructed_id => Atom.to_string(node())}
      _ -> %{fixture.w2 => Atom.to_string(node())}
    end
  end

  defp start_materializer(path, id) do
    name = otp_name_for_worker(id)

    {:ok, pid} =
      start_supervised(%{
        id: id,
        start: {GenServer, :start_link, [Olivine, {name, self(), id, Path.join(path, id), [shard_id: 0]}, [name: name]]}
      })

    assert_receive {:"$gen_cast", {:worker_health, ^id, {:ok, ^pid}}}, 5_000
    pid
  end

  defp unlock(pid, epoch, log, rv) do
    {:ok, ^pid, _} = Materializer.lock_for_recovery(pid, epoch)
    :ok = Materializer.unlock_after_recovery(pid, rv, [{"log", log}])
  end

  defp push(shard, version, mutations) do
    version = Version.from_integer(version)
    encoded = Transaction.encode(%{mutations: Enum.to_list(mutations), commit_version: version})
    ShardServer.push(shard, version, encoded, version)
  end
end
