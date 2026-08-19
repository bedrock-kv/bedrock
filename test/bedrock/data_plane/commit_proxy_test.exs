defmodule Bedrock.DataPlane.CommitProxyTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout

  # Mock GenServer for testing API functions
  defmodule MockCommitProxy do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, %{}, opts)

    def init(state), do: {:ok, state}

    def handle_call({:recover_from, _lock_token, _sequencer, _resolver_layout, _routing_data}, _from, state) do
      {:reply, :ok, state}
    end

    def handle_call({:commit, _epoch, _transaction, mode}, _from, state) when mode in [:user, :system] do
      {:reply, {:ok, 1, 0}, state}
    end
  end

  describe "recover_from/5" do
    test "calls the underlying GenServer with recover_from message" do
      {:ok, pid} = MockCommitProxy.start_link([])

      sequencer = self()
      resolver_layout = %ResolverLayout.Single{resolver_ref: self()}

      routing_snapshot = %{
        shard_layout: %{},
        log_map: %{},
        log_services: %{"test_log" => self()},
        replication_factor: 1
      }

      assert :ok = CommitProxy.recover_from(pid, "test_lock_token", sequencer, resolver_layout, routing_snapshot)
    end
  end

  describe "commit/4" do
    test "sends user mode by default and the given mode when provided" do
      {:ok, pid} = MockCommitProxy.start_link([])

      assert {:ok, 1, 0} = CommitProxy.commit(pid, 1, "tx")
      assert {:ok, 1, 0} = CommitProxy.commit(pid, 1, "tx", mode: :system)
    end

    test "raises on an invalid commit mode at the call site" do
      {:ok, pid} = MockCommitProxy.start_link([])

      assert_raise ArgumentError, "invalid commit mode: :bogus", fn ->
        CommitProxy.commit(pid, 1, "tx", mode: :bogus)
      end
    end
  end
end
