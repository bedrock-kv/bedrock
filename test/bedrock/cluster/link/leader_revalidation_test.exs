defmodule Bedrock.Cluster.Link.LeaderRevalidationTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Cluster.Link.Discovery
  alias Bedrock.Cluster.Link.RoutingCache
  alias Bedrock.Cluster.Link.Server
  alias Bedrock.Cluster.Link.State

  defmodule Cluster do
    @moduledoc false
    def name, do: "v65"
    def otp_name(component), do: :"v65_#{component}"
    def coordinator_ping_timeout_in_ms, do: 100
    def gateway_ping_timeout_in_ms, do: 50
  end

  defmodule FakeCoordinator do
    @moduledoc false
    use GenServer

    def start_link(opts) do
      case opts[:name] do
        nil -> GenServer.start_link(__MODULE__, opts)
        name -> GenServer.start_link(__MODULE__, opts, name: name)
      end
    end

    @impl true
    def init(opts), do: {:ok, %{epoch: opts[:epoch], leader?: Keyword.get(opts, :leader?, true)}}

    @impl true
    def handle_call(:ping, _from, t) do
      leader = if t.leader?, do: self()
      {:reply, {:pong, t.epoch, leader}, t}
    end
  end

  defp link_state(fields) do
    struct!(
      %State{
        node: Node.self(),
        cluster: Cluster,
        descriptor: Descriptor.new("v65", [Node.self()]),
        known_coordinator: :unavailable,
        capabilities: []
      },
      fields
    )
  end

  describe "leader revalidation against the coordinator set" do
    test "abandons a partitioned-but-alive leader once the set reports a newer epoch" do
      # A: the leader we are pinned to. Partitioned from B but perfectly
      # alive, and still answering as leader of its own (old) epoch.
      {:ok, old_leader} = start_supervised(Supervisor.child_spec({FakeCoordinator, epoch: 5}, id: :a))

      # B: elected in a newer epoch, reachable through the coordinator set
      # named in the descriptor.
      {:ok, new_leader} =
        start_supervised(
          Supervisor.child_spec({FakeCoordinator, [epoch: 7, name: Cluster.otp_name(:coordinator)]}, id: :b)
        )

      assert {%State{known_coordinator: ^new_leader}, :ok} =
               Discovery.find_a_live_coordinator(link_state(known_coordinator: old_leader))
    end

    test "keeps polling after a successful discovery" do
      {:ok, _leader} =
        start_supervised({FakeCoordinator, [epoch: 7, name: Cluster.otp_name(:coordinator)]})

      assert {%State{timers: %{find_a_live_coordinator: _}}, :ok} =
               Discovery.find_a_live_coordinator(link_state([]))
    end

    test "keeps the pinned leader when the set has nothing newer to say" do
      # No coordinator is registered, so every member of the set is silent.
      # Silence is not evidence of a new leader.
      {:ok, old_leader} = start_supervised({FakeCoordinator, epoch: 5})

      assert {%State{known_coordinator: ^old_leader}, {:error, :unavailable}} =
               Discovery.find_a_live_coordinator(link_state(known_coordinator: old_leader))
    end
  end

  describe "wiring pushes from a superseded leader" do
    setup do
      %{state: link_state(routing_table: RoutingCache.new(:v65_link_routing))}
    end

    test "drops a push that carries an epoch we have already passed", %{state: state} do
      state = %{state | transaction_system_layout: %{epoch: 7}}
      RoutingCache.insert(state.routing_table, "a", "b", :materializer)

      assert {:noreply, %State{transaction_system_layout: %{epoch: 7}}} =
               Server.handle_info({:tsl_updated, %{epoch: 5}}, state)

      assert {:ok, _entry} = RoutingCache.lookup(state.routing_table, "a")
    end

    test "installs a push that carries a newer epoch", %{state: state} do
      state = %{state | transaction_system_layout: %{epoch: 7}}
      RoutingCache.insert(state.routing_table, "a", "b", :materializer)

      assert {:noreply, %State{transaction_system_layout: %{epoch: 9}}} =
               Server.handle_info({:tsl_updated, %{epoch: 9}}, state)

      assert :not_cached = RoutingCache.lookup(state.routing_table, "a")
    end

    test "installs a clear, which carries no epoch to compare", %{state: state} do
      state = %{state | transaction_system_layout: %{epoch: 7}}

      assert {:noreply, %State{transaction_system_layout: nil}} =
               Server.handle_info({:tsl_updated, nil}, state)
    end
  end
end
