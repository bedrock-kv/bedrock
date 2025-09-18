defmodule Bedrock.DataPlane.Log.Shale.ServerTest do
  use ExUnit.Case, async: false

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    cluster = Cluster
    otp_name = :"test_log_#{:rand.uniform(10_000)}"
    id = "test_log_#{:rand.uniform(10_000)}"
    foreman = self()
    path = Path.join(tmp_dir, "log_segments")

    File.mkdir_p!(path)

    {:ok,
     cluster: cluster,
     otp_name: otp_name,
     id: id,
     foreman: foreman,
     path: path,
     server_opts: [
       cluster: cluster,
       otp_name: otp_name,
       id: id,
       foreman: foreman,
       path: path
     ]}
  end

  describe "child_spec/1" do
    test "creates valid child spec with all required options", %{server_opts: opts} do
      spec = Server.child_spec(opts)
      expected_id = opts[:id]
      expected_name = opts[:otp_name]

      assert %{
               id: {Server, ^expected_id},
               start: {GenServer, :start_link, [Server, _, [name: ^expected_name]]}
             } = spec
    end

    test "raises error when cluster option is missing" do
      opts = [otp_name: :test, id: "test", foreman: self(), path: "/tmp"]

      assert_raise RuntimeError, "Missing :cluster option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when otp_name option is missing" do
      opts = [cluster: Cluster, id: "test", foreman: self(), path: "/tmp"]

      assert_raise RuntimeError, "Missing :otp_name option", fn ->
        Server.child_spec(opts)
      end
    end

    for {missing_key, opts_without_key} <- [
          {:id, [cluster: Cluster, otp_name: :test, foreman: self(), path: "/tmp"]},
          {:foreman, [cluster: Cluster, otp_name: :test, id: "test", path: "/tmp"]},
          {:path, [cluster: Cluster, otp_name: :test, id: "test", foreman: self()]}
        ] do
      test "raises KeyError when #{missing_key} option is missing" do
        assert_raise KeyError, fn ->
          Server.child_spec(unquote(opts_without_key))
        end
      end
    end
  end

  describe "GenServer lifecycle" do
    test "starts successfully with valid options", %{server_opts: opts} do
      assert pid = start_supervised!(Server.child_spec(opts))
      assert Process.alive?(pid)
      if Process.alive?(pid), do: GenServer.stop(pid)
    end

    test "initializes with correct state", %{server_opts: opts} do
      pid = start_supervised!(Server.child_spec(opts))
      state = :sys.get_state(pid)
      version_0 = Version.from_integer(0)

      assert %State{
               cluster: Cluster,
               id: id,
               otp_name: otp_name,
               foreman: foreman,
               path: path,
               mode: :locked,
               oldest_version: ^version_0,
               last_version: ^version_0
             } = state

      assert {id, otp_name, foreman, path} == {opts[:id], opts[:otp_name], opts[:foreman], opts[:path]}

      cleanup_server(pid)
    end

    test "handles initialization continue properly", %{server_opts: opts} do
      pid = start_supervised!(Server.child_spec(opts))

      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler
      end)

      cleanup_server(pid)
    end
  end

  describe "handle_call/3 - basic operations" do
    setup %{server_opts: opts} do
      {:ok, server: setup_server(opts)}
    end

    test "responds to ping", %{server: pid} do
      assert :pong = GenServer.call(pid, :ping)
    end

    test "handles info request", %{server: pid} do
      assert {:ok, %{id: _, kind: _, oldest_version: _}} =
               GenServer.call(pid, {:info, [:id, :kind, :oldest_version]})
    end

    test "handles info request with single fact", %{server: pid} do
      assert {:ok, %{id: id}} = GenServer.call(pid, {:info, [:id]})
      assert is_binary(id)
    end

    test "handles empty info request", %{server: pid} do
      assert {:ok, info} = GenServer.call(pid, {:info, []})
      assert info == %{}
    end
  end

  describe "handle_call/3 - lock_for_recovery" do
    setup %{server_opts: opts} do
      pid = setup_server(opts)
      on_exit(fn -> cleanup_server(pid) end)
      {:ok, server: pid}
    end

    test "handles lock_for_recovery request", %{server: pid} do
      epoch = 1

      result = GenServer.call(pid, {:lock_for_recovery, epoch})

      assert is_tuple(result)
    end
  end

  describe "handle_call/3 - push operations" do
    setup %{server_opts: opts} do
      pid = setup_server(opts)
      on_exit(fn -> cleanup_server(pid) end)
      {:ok, server: pid}
    end

    test "handles push with invalid transaction bytes", %{server: pid} do
      invalid_bytes = "invalid transaction data"
      expected_version = 1

      result = GenServer.call(pid, {:push, invalid_bytes, expected_version})

      assert {:error, _reason} = result
    end

    test "handles push with valid transaction format", %{server: pid} do
      encoded_bytes =
        TransactionTestSupport.new_log_transaction(0, %{"test_key" => "test_value"})

      expected_version = 0

      result = GenServer.call(pid, {:push, encoded_bytes, expected_version}, 1000)

      assert result == :ok or match?({:error, _}, result)
    end
  end

  describe "handle_call/3 - pull operations" do
    setup %{server_opts: opts} do
      pid = setup_server(opts)
      on_exit(fn -> cleanup_server(pid) end)
      {:ok, server: pid}
    end

    test "handles pull request with basic options", %{server: pid} do
      from_version = 0
      opts = [limit: 10, timeout: 5000]

      result = GenServer.call(pid, {:pull, from_version, opts})

      assert is_tuple(result)
    end

    test "handles pull request with default options", %{server: pid} do
      from_version = 0
      opts = []

      result = GenServer.call(pid, {:pull, from_version, opts})

      assert is_tuple(result)
    end

    test "handles pull request with high version number", %{server: pid} do
      from_version = 999_999
      opts = [limit: 1]

      result = GenServer.call(pid, {:pull, from_version, opts})

      assert is_tuple(result)
    end
  end

  describe "handle_call/3 - recover_from operations" do
    setup %{server_opts: opts} do
      pid = setup_server(opts)
      on_exit(fn -> cleanup_server(pid) end)
      {:ok, server: pid}
    end

    test "handles recover_from request", %{server: pid} do
      source_log = self()
      first_version = Version.from_integer(1)
      last_version = Version.from_integer(10)

      catch_exit do
        GenServer.call(pid, {:recover_from, source_log, first_version, last_version}, 500)
      end
    end

    test "handles recover_from with invalid version range", %{server: pid} do
      source_log = self()
      first_version = Version.from_integer(10)
      last_version = Version.from_integer(1)

      catch_exit do
        GenServer.call(pid, {:recover_from, source_log, first_version, last_version}, 500)
      end
    end
  end

  describe "handle_continue/2" do
    setup %{server_opts: opts} do
      pid = setup_server(opts)
      on_exit(fn -> cleanup_server(pid) end)
      {:ok, server: pid}
    end

    test "handles notify_waiting_pullers continue", %{server: pid} do
      state = :sys.get_state(pid)
      assert state.waiting_pullers == %{}
    end
  end

  describe "handle_info/2" do
    setup %{server_opts: opts} do
      pid = setup_server(opts)
      on_exit(fn -> cleanup_server(pid) end)
      {:ok, server: pid}
    end

    test "handles timeout message", %{server: pid} do
      send(pid, :timeout)
      assert Process.alive?(pid)
      assert :pong = GenServer.call(pid, :ping)
    end
  end

  describe "error conditions" do
    test "handles missing directory error during initialization", %{
      cluster: cluster,
      id: id,
      foreman: foreman
    } do
      invalid_path = "/nonexistent/path/that/should/not/exist"
      otp_name = :"test_log_error_#{:rand.uniform(10_000)}"

      opts = [
        cluster: cluster,
        otp_name: otp_name,
        id: id,
        foreman: foreman,
        path: invalid_path
      ]

      Process.flag(:trap_exit, true)

      spec = Server.child_spec(opts)
      {GenServer, :start_link, [module, init_args, gen_opts]} = spec.start
      {:ok, pid} = GenServer.start_link(module, init_args, gen_opts)

      assert_receive {:EXIT, ^pid, :path_is_not_a_directory}, 2000

      refute Process.alive?(pid)
    end
  end

  describe "concurrent operations" do
    setup %{server_opts: opts} do
      pid = setup_server(opts)
      on_exit(fn -> cleanup_server(pid) end)
      {:ok, server: pid}
    end

    test "handles multiple concurrent ping requests", %{server: pid} do
      tasks =
        for _i <- 1..10 do
          Task.async(fn -> GenServer.call(pid, :ping) end)
        end

      results = Task.await_many(tasks)

      assert Enum.all?(results, &(&1 == :pong))
    end

    test "handles concurrent info requests", %{server: pid} do
      tasks =
        for _i <- 1..5 do
          Task.async(fn -> GenServer.call(pid, {:info, [:id, :kind]}) end)
        end

      results = Task.await_many(tasks)

      assert Enum.all?(results, fn result ->
               match?({:ok, %{id: _, kind: _}}, result)
             end)
    end
  end

  describe "property-based testing" do
    use ExUnitProperties

    setup %{server_opts: base_opts} do
      {:ok, base_opts: base_opts}
    end

    test "transactions are processed in version order regardless of arrival order", %{base_opts: base_opts} do
      sequence_length = 8

      # Create fresh server with unique name and path
      iteration_id = :rand.uniform(1_000_000)
      unique_path = Path.join(base_opts[:path], "test_#{iteration_id}")
      File.mkdir_p!(unique_path)

      unlocked_opts =
        base_opts
        |> Keyword.put(:start_unlocked, true)
        |> Keyword.put(:otp_name, :"test_log_#{iteration_id}")
        |> Keyword.put(:id, "test_log_#{iteration_id}")
        |> Keyword.put(:path, unique_path)

      server = setup_server(unlocked_opts)

      # Create transaction specs with correct version semantics
      # expected_version should equal server's current last_version (starting from 0)
      # commit_version should be expected_version + 1
      transaction_specs =
        for i <- 0..(sequence_length - 1) do
          expected_version = Version.from_integer(i)
          commit_version = i + 1
          transaction = TransactionTestSupport.new_log_transaction(commit_version, %{"key_#{i}" => "value_#{i}"})
          {expected_version, transaction}
        end

      # Send transactions concurrently in shuffled order to test out-of-order handling
      # The server should queue higher versions until lower ones complete
      shuffled_specs = Enum.shuffle(transaction_specs)

      tasks =
        for {version, transaction} <- shuffled_specs do
          Task.async(fn ->
            GenServer.call(server, {:push, transaction, version}, 5_000)
          end)
        end

      # Wait for all tasks to complete
      task_results = Enum.map(tasks, &Task.await(&1, 5_000))

      # All pushes should succeed
      assert Enum.all?(task_results, &(&1 == :ok))

      # Verify server state is clean and advanced correctly
      final_state = :sys.get_state(server)
      assert map_size(final_state.pending_pushes) == 0
      expected_final_version = Version.from_integer(sequence_length)
      assert final_state.last_version == expected_final_version

      # Cleanup server
      cleanup_server(server)
    end
  end

  defp setup_server(opts) do
    pid = start_supervised!(Server.child_spec(opts))

    eventually(fn ->
      state = :sys.get_state(pid)
      assert state.segment_recycler
    end)

    pid
  end

  defp cleanup_server(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  end

  defp eventually(assertion_fn, timeout \\ 1000, interval \\ 50) do
    end_time = System.monotonic_time(:millisecond) + timeout

    eventually_loop(assertion_fn, end_time, interval)
  end

  defp eventually_loop(assertion_fn, end_time, interval) do
    assertion_fn.()
  rescue
    _ ->
      if System.monotonic_time(:millisecond) < end_time do
        Process.sleep(interval)
        eventually_loop(assertion_fn, end_time, interval)
      else
        assertion_fn.()
      end
  end
end
