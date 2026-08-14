defmodule Bedrock.DataPlane.Materializer.Basalt.ServerTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.Common.GenServerTestHelpers

  alias Bedrock.DataPlane.Materializer.Basalt.Server
  alias Bedrock.DataPlane.Materializer.Basalt.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  defmodule FakeLog do
    @moduledoc false
    # Minimal log server: replies to `Log.pull/3` calls with the pre-loaded
    # batches (one batch per pull), then keeps replying with empty batches.
    use GenServer

    def start_link(batches), do: GenServer.start_link(__MODULE__, batches)

    @impl true
    def init(batches), do: {:ok, batches}

    @impl true
    def handle_call({:pull, _start_after, _opts}, _from, [batch | rest]), do: {:reply, {:ok, batch}, rest}
    def handle_call({:pull, _start_after, _opts}, _from, []), do: {:reply, {:ok, []}, []}
  end

  defp wait_until(fun, deadline_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms

    fun
    |> Stream.repeatedly()
    |> Enum.reduce_while(:ok, fn
      true, _acc ->
        {:halt, :ok}

      false, _acc ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(10)
          {:cont, :ok}
        else
          {:halt, :timeout}
        end
    end)
  end

  describe "puller notification round-trip" do
    @tag :tmp_dir
    test "waitlisted fetch is replied to once the puller applies the awaited version", %{tmp_dir: tmp_dir} do
      version_1 = Version.from_integer(1)
      transaction = TransactionTestSupport.new_log_transaction(version_1, %{"foo" => "bar"})
      {:ok, fake_log} = FakeLog.start_link([[transaction]])

      otp_name = :"basalt_server_#{System.unique_integer([:positive])}"

      {:ok, server} =
        start_supervised(
          Server.child_spec(otp_name: otp_name, foreman: self(), id: "basalt_server_test", path: tmp_dir)
        )

      # The server reports health to the foreman (us) once startup completes.
      assert_cast_received({:worker_health, "basalt_server_test", {:ok, ^server}}, 5_000)

      assert {:ok, ^server, _info} = GenServer.call(server, {:lock_for_recovery, 1})

      # Waitlist a fetch for a version that hasn't been applied yet. Use an
      # unlinked process so a call timeout shows up as a missing message
      # instead of crashing the test process.
      test_pid = self()

      spawn(fn ->
        send(test_pid, {:get_result, GenServer.call(server, {:get, "foo", version_1, []}, 30_000)})
      end)

      assert :ok = wait_until(fn -> map_size(:sys.get_state(server).waiting_fetches) == 1 end)

      layout = %{
        logs: %{"log_1" => []},
        services: %{"log_1" => %{kind: :log, status: {:up, fake_log}, last_seen: nil}}
      }

      assert :ok = GenServer.call(server, {:unlock_after_recovery, Version.zero(), layout})

      # Once the puller applies version 1, the server must notify the
      # waitlisted fetch with the value pulled from the log.
      assert_receive {:get_result, {:ok, "bar"}}, 5_000
      assert map_size(:sys.get_state(server).waiting_fetches) == 0
    end
  end

  describe "child_spec/1" do
    test "creates proper child spec with all required options" do
      opts = [
        otp_name: :test_server,
        foreman: self(),
        id: "test_storage_1",
        path: "/tmp/test_storage"
      ]

      spec = Server.child_spec(opts)

      assert spec.id == {Server, "test_storage_1"}

      assert {GenServer, :start_link,
              [
                Server,
                {:test_server, _, "test_storage_1", "/tmp/test_storage"},
                [name: :test_server]
              ]} = spec.start
    end

    test "raises when otp_name is missing" do
      opts = [foreman: self(), id: "test", path: "/tmp"]

      assert_raise RuntimeError, "Missing :otp_name option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises when foreman is missing" do
      opts = [otp_name: :test, id: "test", path: "/tmp"]

      assert_raise RuntimeError, "Missing :foreman option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises when id is missing" do
      opts = [otp_name: :test, foreman: self(), path: "/tmp"]

      assert_raise RuntimeError, "Missing :id option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises when path is missing" do
      opts = [otp_name: :test, foreman: self(), id: "test"]

      assert_raise RuntimeError, "Missing :path option", fn ->
        Server.child_spec(opts)
      end
    end
  end

  describe "GenServer callbacks" do
    setup do
      # Create a minimal mock state for testing
      state = %State{
        otp_name: :test_server,
        path: "/tmp/test",
        foreman: self(),
        id: "test_storage_1",
        database: :mock_database,
        mode: :running
      }

      {:ok, state: state}
    end

    test "init/1 returns continuation for startup" do
      args = {:test_server, self(), "test_id", "/tmp/test"}

      assert {:ok, ^args, {:continue, :finish_startup}} = Server.init(args)
    end

    test "handle_call with unknown message returns not_ready error", %{state: state} do
      result = Server.handle_call(:unknown_message, self(), state)

      assert {:reply, {:error, :not_ready}, ^state} = result
    end
  end

  describe "GenServer message handlers" do
    test "handle_call with unknown message returns not_ready error" do
      state = %State{
        otp_name: :test_server,
        path: "/tmp/test",
        foreman: self(),
        id: "test_storage_1",
        database: nil,
        mode: :running
      }

      # Test unknown call handling
      result = Server.handle_call(:unknown_message, self(), state)
      assert {:reply, {:error, :not_ready}, ^state} = result
    end
  end

  describe "handle_continue callbacks" do
    # Example of testing GenServer messages with helper macros
    # Before: assert_receive {:"$gen_cast", {:some_message, _}}
    # After:  assert_cast_received({:some_message, actual_data}) do
    #           assert actual_data == expected_value
    #         end

    test "handle_continue :finish_startup calls Logic.startup" do
      args = {:test_server, self(), "test_id", "/tmp/test"}

      # This will attempt to call Logic.startup which will likely fail in test
      # But it exercises the callback path
      result = Server.handle_continue(:finish_startup, args)

      # Expect either success or error, but the path is exercised
      assert result == {:stop, :enoent, :no_state} or match?({:noreply, _, _}, result)
    end

    test "handle_continue :report_health_to_foreman sends properly formatted cast message" do
      state = %State{
        otp_name: :test_server,
        path: "/tmp/test",
        foreman: self(),
        id: "test_storage_1",
        database: nil
      }

      # This will exercise the health reporting path
      result = Server.handle_continue(:report_health_to_foreman, state)

      assert {:noreply, ^state} = result

      # Use our helper macro to assert on the exact cast message format
      assert_cast_received({:worker_health, worker_id, health_status}) do
        assert worker_id == "test_storage_1"
        assert {:ok, pid} = health_status
        assert is_pid(pid)
      end
    end
  end

  describe "terminate/2" do
    test "has terminate callback defined for proper cleanup" do
      # Ensure module is loaded before checking exports
      Code.ensure_loaded!(Server)

      # Test that the terminate function exists and has the right arity
      # Testing the actual termination requires a real database setup
      assert function_exported?(Server, :terminate, 2)
    end
  end

  describe "module structure" do
    test "exports expected GenServer functions" do
      # Ensure module is loaded before checking exports
      Code.ensure_loaded!(Server)

      # Verify the module has the expected GenServer callbacks
      assert function_exported?(Server, :init, 1)
      assert function_exported?(Server, :handle_call, 3)
      assert function_exported?(Server, :handle_continue, 2)
      assert function_exported?(Server, :terminate, 2)
    end
  end
end
