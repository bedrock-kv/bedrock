defmodule Bedrock.ControlPlane.Coordinator.State do
  @moduledoc false

  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator.RecoveryCapabilityTracker
  alias Bedrock.ControlPlane.Director
  alias Bedrock.Raft

  @type leader_startup_state :: :not_leader | :leader_waiting_consensus | :leader_ready | :recovery_failed

  @type t :: %__MODULE__{
          cluster: module(),
          leader_node: node() | :undecided,
          my_node: node(),
          epoch: Bedrock.epoch(),
          director: Director.ref() | :unavailable,
          otp_name: atom(),
          raft: Raft.t(),
          supervisor_otp_name: atom(),
          last_durable_txn_id: Raft.transaction_id(),
          config: Config.t() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          waiting_list: %{Raft.transaction_id() => pid()},
          service_directory: %{String.t() => {atom(), {atom(), node()}}},
          node_capabilities: %{node() => [Cluster.capability()]},
          tsl_subscribers: MapSet.t(pid()),
          leader_startup_state: leader_startup_state(),
          recovery_tracker: RecoveryCapabilityTracker.t()
        }
  defstruct cluster: nil,
            leader_node: :undecided,
            my_node: nil,
            epoch: nil,
            director: :unavailable,
            otp_name: nil,
            raft: nil,
            supervisor_otp_name: nil,
            last_durable_txn_id: nil,
            config: nil,
            transaction_system_layout: nil,
            waiting_list: %{},
            service_directory: %{},
            node_capabilities: %{},
            tsl_subscribers: MapSet.new(),
            leader_startup_state: :not_leader,
            recovery_tracker: %RecoveryCapabilityTracker{}

  defmodule Changes do
    @moduledoc false

    alias Bedrock.ControlPlane.Coordinator.State

    @spec put_epoch(t :: State.t(), epoch :: Bedrock.epoch()) :: State.t()
    def put_epoch(t, epoch), do: %{t | epoch: epoch}

    @spec put_director(t :: State.t(), new_director :: Director.ref() | :unavailable) ::
            State.t()
    def put_director(t, new_director), do: %{t | director: new_director}

    @spec put_leader_node(t :: State.t(), leader_node :: node() | :undecided) :: State.t()
    def put_leader_node(t, leader_node), do: %{t | leader_node: leader_node}

    @spec put_leader_startup_state(t :: State.t(), State.leader_startup_state()) :: State.t()
    def put_leader_startup_state(t, leader_startup_state), do: %{t | leader_startup_state: leader_startup_state}

    @spec put_recovery_tracker(t :: State.t(), RecoveryCapabilityTracker.t()) :: State.t()
    def put_recovery_tracker(t, recovery_tracker), do: %{t | recovery_tracker: recovery_tracker}

    @spec update_recovery_tracker(t :: State.t(), (RecoveryCapabilityTracker.t() -> RecoveryCapabilityTracker.t())) ::
            State.t()
    def update_recovery_tracker(t, updater), do: %{t | recovery_tracker: updater.(t.recovery_tracker)}

    @spec set_raft(t :: State.t(), Raft.t()) :: State.t()
    def set_raft(t, raft), do: %{t | raft: raft}

    @spec update_raft(t :: State.t(), updater :: (Raft.t() -> Raft.t())) :: State.t()
    def update_raft(t, updater), do: %{t | raft: updater.(t.raft)}

    @spec put_config(t :: State.t(), Config.t()) :: State.t()
    def put_config(t, config), do: %{t | config: config}

    @spec update_config(t :: State.t(), updater :: (Config.t() -> Config.t())) :: State.t()
    def update_config(t, updater), do: %{t | config: updater.(t.config)}

    @spec put_last_durable_txn_id(t :: State.t(), Raft.transaction_id()) :: State.t()
    def put_last_durable_txn_id(t, last_durable_txn_id), do: %{t | last_durable_txn_id: last_durable_txn_id}

    @spec put_transaction_system_layout(t :: State.t(), TransactionSystemLayout.t()) ::
            State.t()
    def put_transaction_system_layout(t, transaction_system_layout) do
      updated_state = %{t | transaction_system_layout: transaction_system_layout}
      broadcast_tsl_update(updated_state, transaction_system_layout)
    end

    @spec put_service_directory(t :: State.t(), %{String.t() => {atom(), {atom(), node()}}}) ::
            State.t()
    def put_service_directory(t, service_directory), do: %{t | service_directory: service_directory}

    @spec update_service_directory(
            t :: State.t(),
            updater :: (%{String.t() => {atom(), {atom(), node()}}} ->
                          %{String.t() => {atom(), {atom(), node()}}})
          ) :: State.t()
    def update_service_directory(t, updater), do: %{t | service_directory: updater.(t.service_directory)}

    @spec add_tsl_subscriber(t :: State.t(), subscriber :: pid()) :: State.t()
    def add_tsl_subscriber(t, subscriber), do: %{t | tsl_subscribers: MapSet.put(t.tsl_subscribers, subscriber)}

    @spec remove_tsl_subscriber(t :: State.t(), subscriber :: pid()) :: State.t()
    def remove_tsl_subscriber(t, subscriber), do: %{t | tsl_subscribers: MapSet.delete(t.tsl_subscribers, subscriber)}

    @spec broadcast_tsl_update(t :: State.t(), tsl :: TransactionSystemLayout.t() | nil) :: State.t()
    def broadcast_tsl_update(t, tsl) do
      for subscriber <- t.tsl_subscribers do
        send(subscriber, {:tsl_updated, tsl})
      end

      t
    end

    @spec update_node_capabilities(t :: State.t(), node(), [Cluster.capability()]) :: State.t()
    def update_node_capabilities(t, node, capabilities),
      do: %{t | node_capabilities: Map.put(t.node_capabilities, node, capabilities)}

    @spec convert_to_capability_map(%{node() => [Cluster.capability()]}) :: %{
            Cluster.capability() => [node()]
          }
    def convert_to_capability_map(node_capabilities) do
      capability_map =
        node_capabilities
        |> Enum.flat_map(fn {node, capabilities} ->
          Enum.map(capabilities, fn capability -> {capability, node} end)
        end)
        |> Enum.group_by(fn {capability, _node} -> capability end, fn {_capability, node} ->
          node
        end)
        |> Map.new(fn {capability, nodes} ->
          # Filter out dead nodes
          live_nodes = Enum.filter(nodes, &(Node.ping(&1) == :pong))
          {capability, live_nodes}
        end)

      # For now, set resolution capable nodes to the same as coordination capable nodes
      coordination_nodes = Map.get(capability_map, :coordination, [])
      Map.put(capability_map, :resolution, coordination_nodes)
    end

    @spec check_for_recovery_capability_changes(State.t()) ::
            {:changed | :unchanged, State.t()}
    def check_for_recovery_capability_changes(t) do
      case RecoveryCapabilityTracker.check_for_recovery_state_changes(
             t.recovery_tracker,
             t.node_capabilities,
             t.service_directory
           ) do
        {:changed, new_tracker} ->
          {:changed, put_recovery_tracker(t, new_tracker)}

        {:unchanged, tracker} ->
          {:unchanged, put_recovery_tracker(t, tracker)}
      end
    end

    @spec update_recovery_capability_hash(State.t()) :: State.t()
    def update_recovery_capability_hash(t) do
      new_tracker =
        RecoveryCapabilityTracker.update_recovery_state_hash(
          t.recovery_tracker,
          t.node_capabilities,
          t.service_directory
        )

      put_recovery_tracker(t, new_tracker)
    end
  end
end
