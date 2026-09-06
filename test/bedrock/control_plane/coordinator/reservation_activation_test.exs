defmodule Bedrock.ControlPlane.Coordinator.ReservationActivationTest do
  use ExUnit.Case, async: true

  alias Bedrock.ClusterBootstrap.Publication
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.RecoveryGeneration
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft
  alias Bedrock.Raft.Log.InMemoryLog

  defmodule Cluster do
    @moduledoc false
    def name, do: "reservation_activation"
    def fetch_coordinator_nodes, do: {:ok, [node()]}
    def node_config, do: []
  end

  test "reserved initial metadata still selects first recovery initialization" do
    alias Bedrock.ControlPlane.Config.RecoveryAttempt
    alias Bedrock.ControlPlane.Director.Recovery.InitializationPhase
    alias Bedrock.ControlPlane.Director.Recovery.TSLValidationPhase

    prior = CoreState.from_bootstrap(%{logs: [], epoch: 1})
    attempt = %RecoveryAttempt{epoch: 2}
    assert {^attempt, InitializationPhase} = TSLValidationPhase.execute(attempt, %{prior_core_state: prior})
  end

  test "bootstrap rejects invalid completed epoch, materializer and config metadata" do
    initial = %{cluster_id: "valid", epoch: 7, logs: [%{id: "log"}], coordinators: [%{node: "node@host"}]}
    assert :ok = Publication.validate(initial)

    for invalid <- [
          Map.put(initial, :epoch, 0),
          Map.put(initial, :logs, [%{id: "log", shard_tags: [0]}, %{id: "log", shard_tags: [1]}]),
          Map.put(initial, :system_materializers, [%{id: "mat", node: ""}]),
          Map.put(initial, :parameters, %{ping_rate_in_hz: 0}),
          Map.put(initial, :policies, %{allow_volunteer_nodes_to_join: :yes})
        ] do
      assert {:error, :invalid_bootstrap} = Publication.validate(invalid), inspect(invalid)
    end
  end

  test "serialized malformed legacy metadata is rejected before recovery projection" do
    alias Bedrock.SystemKeys.ClusterBootstrap

    valid = %{cluster_id: "wire", epoch: 7, logs: [%{id: "log"}], coordinators: [%{node: "n@host"}]}

    for invalid <- [
          %{valid | epoch: 0},
          Map.put(valid, :system_materializers, [%{id: "", node: ""}]),
          %{valid | logs: [%{id: "log", shard_tags: [0]}, %{id: "log", shard_tags: [1]}]}
        ] do
      assert {:error, :invalid_bootstrap} = invalid |> ClusterBootstrap.to_binary() |> Publication.decode()
    end
  end

  test "reservation activation adopts committed config from its exact prior bytes" do
    raft = node() |> Raft.new([], InMemoryLog.new(:tuple), RaftAdapter) |> Raft.handle_event(:election, :timer)
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one, max_children: 0})
    io_id = make_ref()

    request = %{
      phase: :reserving,
      request_id: "new",
      owner_term: 1,
      io_id: io_id,
      worker: nil,
      monitor: nil,
      timer: nil
    }

    prior = %{
      cluster_id: "valid",
      epoch: 7,
      logs: [%{id: "winner"}],
      coordinators: [%{node: Atom.to_string(node())}],
      parameters: %{desired_logs: 3},
      policies: %{allow_volunteer_nodes_to_join: false}
    }

    reservation = %{generation: 8, recovery_id: "new", prior_bootstrap: prior}

    state = %State{
      cluster: Cluster,
      raft: raft,
      raft_term: 1,
      leader_node: node(),
      my_node: node(),
      supervisor_otp_name: sup,
      recovery_generation: request,
      config: Config.new([node()])
    }

    activated = RecoveryGeneration.result(state, io_id, {:ok, reservation})
    assert activated.prior_core_state.logs == %{"winner" => []}
    assert activated.config.parameters.desired_logs == 3
    assert activated.config.policies.allow_volunteer_nodes_to_join == false
  end
end
