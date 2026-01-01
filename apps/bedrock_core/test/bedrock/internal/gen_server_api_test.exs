defmodule Bedrock.Internal.GenServerApiTest do
  use ExUnit.Case, async: true

  # Test module that uses GenServerApi with :for option
  defmodule TestServer do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
    def init(state), do: {:ok, state}
  end

  defmodule TestApi do
    @moduledoc false
    use Bedrock.Internal.GenServerApi, for: TestServer
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
end
