defmodule Bedrock.Cluster.TSLDeltaRoundTripTest do
  @moduledoc """
  Round-trip test for post-recovery TSL delta application:

      Director.apply_tsl_delta/3
        -> Coordinator {:notify_transaction_system_layout, tsl}
        -> Coordinator broadcasts {:tsl_updated, tsl} to subscribed Links
        -> Link serves the new TSL via fetch_transaction_system_layout/1

  Uses the real `Link.Server` GenServer and thin harness GenServers that
  delegate their callbacks to the real `Coordinator.Server` and
  `Director.Server` handler functions (skipping only their heavyweight
  `init` paths: raft bootstrap and recovery).
  """
  use ExUnit.Case, async: false

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Cluster.Link
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Director

  @coordinator_otp_name :tsl_round_trip_coordinator

  defmodule TestCluster do
    @moduledoc false

    @spec otp_name(atom()) :: atom()
    def otp_name(:coordinator), do: :tsl_round_trip_coordinator
    def otp_name(component), do: :"tsl_round_trip_#{component}"

    @spec gateway_ping_timeout_in_ms() :: pos_integer()
    def gateway_ping_timeout_in_ms, do: 100

    @spec coordinator_ping_timeout_in_ms() :: pos_integer()
    def coordinator_ping_timeout_in_ms, do: 100
  end

  defmodule CoordinatorHarness do
    @moduledoc false
    use GenServer

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    defdelegate handle_call(message, from, state), to: Server

    @impl true
    defdelegate handle_cast(message, state), to: Server

    @impl true
    defdelegate handle_info(message, state), to: Server
  end

  defmodule DirectorHarness do
    @moduledoc false
    use GenServer

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    defdelegate handle_call(message, from, state), to: Bedrock.ControlPlane.Director.Server
  end

  defp base_tsl(epoch) do
    %{
      id: 1,
      epoch: epoch,
      director: nil,
      sequencer: nil,
      rate_keeper: nil,
      proxies: [],
      resolvers: [],
      logs: %{},
      services: %{},
      shard_layout: %{},
      shard_materializers: %{}
    }
  end

  defp start_coordinator(epoch, opts \\ []) do
    state = %Coordinator.State{
      cluster: TestCluster,
      my_node: Node.self(),
      leader_node: Node.self(),
      epoch: epoch,
      otp_name: @coordinator_otp_name,
      transaction_system_layout: Keyword.get(opts, :tsl)
    }

    start_supervised!(%{
      id: CoordinatorHarness,
      start: {GenServer, :start_link, [CoordinatorHarness, state, [name: @coordinator_otp_name]]}
    })
  end

  defp start_director(epoch, coordinator) do
    state = %Director.State{
      state: :running,
      epoch: epoch,
      cluster: TestCluster,
      coordinator: coordinator,
      transaction_system_layout: base_tsl(epoch)
    }

    start_supervised!(%{
      id: DirectorHarness,
      start: {GenServer, :start_link, [DirectorHarness, state]}
    })
  end

  defp start_link_server do
    descriptor = %Descriptor{cluster_name: "tsl_round_trip", coordinator_nodes: [Node.self()]}

    start_supervised!(%{
      id: Link.Server,
      start: {GenServer, :start_link, [Link.Server, {TestCluster, "/dev/null", descriptor, :active, []}]}
    })
  end

  defp eventually(fun, attempts \\ 50)
  defp eventually(fun, 0), do: fun.() || flunk("condition never became true")

  defp eventually(fun, attempts) do
    result = fun.()

    if result do
      result
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  test "director delta application propagates through the coordinator to a live Link" do
    coordinator = start_coordinator(5)
    link = start_link_server()

    eventually(fn -> match?({:ok, _}, Link.fetch_coordinator(link)) end)
    assert {:error, :unavailable} = Link.fetch_transaction_system_layout(link)

    director = start_director(5, coordinator)
    materializer = spawn(fn -> Process.sleep(:infinity) end)

    assert :ok = Director.apply_tsl_delta(director, %{1 => materializer}, 5)

    tsl =
      eventually(fn ->
        case Link.fetch_transaction_system_layout(link) do
          {:ok, tsl} -> tsl
          {:error, :unavailable} -> nil
        end
      end)

    assert tsl.shard_materializers == %{1 => materializer}
    assert tsl.epoch == 5
  end

  test "a delta carrying a stale epoch is rejected and nothing is broadcast" do
    coordinator = start_coordinator(5)
    link = start_link_server()

    eventually(fn -> match?({:ok, _}, Link.fetch_coordinator(link)) end)

    director = start_director(5, coordinator)
    materializer = spawn(fn -> Process.sleep(:infinity) end)

    assert {:error, :newer_epoch_exists} = Director.apply_tsl_delta(director, %{1 => materializer}, 4)

    # Apply a valid delta afterwards; the first TSL the link observes must not
    # contain any effect from the stale delta.
    assert :ok = Director.apply_tsl_delta(director, %{2 => materializer}, 5)

    tsl =
      eventually(fn ->
        case Link.fetch_transaction_system_layout(link) do
          {:ok, tsl} -> tsl
          {:error, :unavailable} -> nil
        end
      end)

    assert tsl.shard_materializers == %{2 => materializer}
  end

  test "a dead subscriber does not break broadcast to remaining subscribers" do
    coordinator = start_coordinator(5)

    dead_link = spawn(fn -> :ok end)
    ref = Process.monitor(dead_link)
    assert_receive {:DOWN, ^ref, :process, ^dead_link, _}

    :ok = Coordinator.subscribe_to_tsl_updates(coordinator, dead_link)
    :ok = Coordinator.subscribe_to_tsl_updates(coordinator, self())

    director = start_director(5, coordinator)
    materializer = spawn(fn -> Process.sleep(:infinity) end)

    assert :ok = Director.apply_tsl_delta(director, %{1 => materializer}, 5)

    assert_receive {:tsl_updated, tsl}
    assert tsl.shard_materializers == %{1 => materializer}
  end

  test "subscribing after a TSL is known delivers the current TSL immediately" do
    coordinator = start_coordinator(5)
    director = start_director(5, coordinator)
    materializer = spawn(fn -> Process.sleep(:infinity) end)

    assert :ok = Director.apply_tsl_delta(director, %{1 => materializer}, 5)

    # Subscribe only after the coordinator already holds a TSL; we should be
    # brought up to date without waiting for the next delta.
    :ok = Coordinator.subscribe_to_tsl_updates(coordinator, self())

    assert_receive {:tsl_updated, tsl}
    assert tsl.shard_materializers == %{1 => materializer}
  end

  test "a notify from a deposed director (older epoch) is dropped by the coordinator" do
    # Coordinator has moved on to epoch 6 (leadership change); a director from
    # epoch 5 still manages to emit a notify (e.g. an in-flight delta that
    # raced the leadership change). The coordinator must not regress its epoch
    # nor broadcast the stale TSL.
    coordinator = start_coordinator(6)
    :ok = Coordinator.subscribe_to_tsl_updates(coordinator, self())

    stale_director = start_director(5, coordinator)
    materializer = spawn(fn -> Process.sleep(:infinity) end)

    # The stale director accepts the delta against its own (old) epoch...
    assert :ok = Director.apply_tsl_delta(stale_director, %{1 => materializer}, 5)

    # ...but the coordinator drops the resulting notify: nothing is broadcast
    # and the coordinator's TSL/epoch are unchanged.
    refute_receive {:tsl_updated, _}, 100
    assert {:ok, nil} = GenServer.call(coordinator, :fetch_transaction_system_layout)
    assert {:pong, 6, _leader} = GenServer.call(coordinator, :ping)
  end

  test "snapshot-on-subscribe does not push the bootstrap-loaded old-TSL stub" do
    # At init the coordinator may hold a partial old-TSL stub loaded from
    # object storage (bare %{logs: ...}, recovery input only). Subscribers
    # must not receive it as if it were a live TSL.
    coordinator = start_coordinator(5, tsl: %{logs: %{"log_1" => [0]}})

    :ok = Coordinator.subscribe_to_tsl_updates(coordinator, self())
    refute_receive {:tsl_updated, _}, 100

    # Once a director produces a real TSL, the subscriber hears about it.
    director = start_director(5, coordinator)
    materializer = spawn(fn -> Process.sleep(:infinity) end)
    assert :ok = Director.apply_tsl_delta(director, %{1 => materializer}, 5)

    assert_receive {:tsl_updated, %{epoch: 5}}
  end

  test "repeated subscriptions do not accumulate duplicate monitors" do
    coordinator = start_coordinator(5)

    :ok = Coordinator.subscribe_to_tsl_updates(coordinator, self())
    :ok = Coordinator.subscribe_to_tsl_updates(coordinator, self())
    :ok = Coordinator.subscribe_to_tsl_updates(coordinator, self())

    # Synchronize on the casts having been processed.
    assert {:pong, 5, _} = GenServer.call(coordinator, :ping)

    {:monitors, monitors} = Process.info(coordinator, :monitors)
    me = self()
    assert Enum.count(monitors, &match?({:process, ^me}, &1)) == 1
  end
end
