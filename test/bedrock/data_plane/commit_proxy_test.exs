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

    def handle_call({:recover_from, _lock_token, _sequencer, _resolver_layout}, _from, state) do
      {:reply, :ok, state}
    end

    def handle_call({:commit, _transaction}, _from, state) do
      {:reply, {:ok, 1, 0}, state}
    end
  end

  describe "recover_from/4" do
    test "calls the underlying GenServer with recover_from message" do
      {:ok, pid} = MockCommitProxy.start_link([])

      sequencer = self()
      resolver_layout = %ResolverLayout.Single{resolver_ref: self()}

      assert :ok = CommitProxy.recover_from(pid, "test_lock_token", sequencer, resolver_layout)
    end
  end
end
