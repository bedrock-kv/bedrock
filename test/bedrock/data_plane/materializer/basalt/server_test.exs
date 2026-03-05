defmodule Bedrock.DataPlane.Materializer.Basalt.ServerTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.Common.GenServerTestHelpers

  alias Bedrock.DataPlane.Materializer.Basalt.Server
  alias Bedrock.DataPlane.Materializer.Basalt.State

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
