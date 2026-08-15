defmodule Bedrock.DataPlane.Materializer.Olivine.PullingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Pulling
  alias Bedrock.DataPlane.Version

  defmodule FloorLog do
    @moduledoc false
    use GenServer

    def start_link(floor), do: GenServer.start_link(__MODULE__, floor)

    @impl true
    def init(floor), do: {:ok, floor}

    @impl true
    def handle_call({:pull, _from_version, _opts}, _from, floor) do
      {:reply, {:error, {:version_too_old, floor}}, floor}
    end
  end

  test "a pull below the WAL floor notifies the owner and ends the puller" do
    floor = Version.from_integer(9_999)
    {:ok, log} = FloorLog.start_link(floor)

    logs = %{"log_1" => []}
    services = %{"log_1" => %{status: {:up, log}}}

    puller =
      Pulling.start_pulling(
        Version.zero(),
        "worker_1",
        logs,
        services,
        fn _transactions -> flunk("must not apply transactions") end,
        fn -> Version.zero() end
      )

    assert_receive {:pull_floor_exceeded, ^floor}, 2_000

    # The puller ends instead of circuit-breaking and retrying forever
    assert {:ok, :ok} = Task.yield(puller, 2_000)
  end
end
