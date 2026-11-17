defmodule Bedrock.Internal.GenServerApiTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.GenServerApi

  # Test module that uses GenServerApi with :for option
  defmodule TestServer do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
    def init(state), do: {:ok, state}
  end

  defmodule TestApi do
    @moduledoc false
    use GenServerApi, for: TestServer
  end

  describe "GenServerApi with :for option" do
    test "provides child_spec/1 delegation" do
      # The child_spec should be delegated to the server module
      assert is_map(TestApi.child_spec([]))
    end

    test "provides start_link/1 wrapper" do
      # Should be able to start via the API module
      {:ok, pid} = TestApi.start_link(%{test: true})
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end
  end

  # Facades whose real GenServer expects a positional tuple, matching the
  # pattern used throughout the data/control plane (Sequencer, Director,
  # Coordinator, Placeholder, ...): child_spec/1 takes keyword opts but
  # init/1 takes a tuple assembled from them.
  defmodule TupleInitServer do
    @moduledoc false
    use GenServer

    def init({a, b}), do: {:ok, {a, b}}
  end

  defmodule TupleInitApi do
    @moduledoc false
    use GenServerApi, for: TupleInitServer
  end

  describe "GenServerApi start_link with a tuple-init server" do
    test "start_link/1 forwards keyword opts unchanged, crashing init/1" do
      Process.flag(:trap_exit, true)

      assert {:error, {:function_clause, _stacktrace}} =
               TupleInitApi.start_link(a: 1, b: 2)
    end

    test "start_link/2 forwards the tuple init arg and GenServer options" do
      {:ok, pid} = TupleInitApi.start_link({1, 2}, name: :gen_server_api_tuple_init_test)
      assert Process.whereis(:gen_server_api_tuple_init_test) == pid
      GenServer.stop(pid)
    end
  end
end
