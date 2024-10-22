defmodule Bedrock.ControlPlane.Coordinator.State do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Config
  alias Bedrock.Raft

  @type t :: %__MODULE__{
          cluster: module(),
          leader_node: node() | :undecided,
          my_node: node(),
          controller: ClusterController.ref() | :unavailable,
          controller_otp_name: atom(),
          otp_name: atom(),
          raft: Raft.t(),
          supervisor_otp_name: atom(),
          last_durable_txn_id: Raft.transaction_id(),
          config: Config.t(),
          waiting_list: %{Raft.transaction_id() => pid()}
        }
  defstruct cluster: nil,
            leader_node: :undecided,
            my_node: nil,
            controller: :unavailable,
            controller_otp_name: nil,
            otp_name: nil,
            raft: nil,
            supervisor_otp_name: nil,
            last_durable_txn_id: nil,
            config: nil,
            waiting_list: %{}

  defmodule Changes do
    alias Bedrock.ControlPlane.Coordinator.State

    @spec set_controller(t :: State.t(), new_controller :: ClusterController.ref() | :unavailable) ::
            State.t()
    def set_controller(t, new_controller),
      do: put_in(t.controller, new_controller)

    @spec set_leader_node(t :: State.t(), leader_node :: node()) :: State.t()
    def set_leader_node(t, leader_node), do: put_in(t.leader_node, leader_node)

    @spec set_raft(t :: State.t(), Raft.t()) :: State.t()
    def set_raft(t, raft), do: put_in(t.raft, raft)

    @spec update_raft(t :: State.t(), updater :: (Raft.t() -> Raft.t())) :: State.t()
    def update_raft(t, updater), do: update_in(t.raft, updater)

    @spec set_config(t :: State.t(), Config.t()) :: State.t()
    def set_config(t, config), do: put_in(t.config, config)

    @spec update_config(t :: State.t(), updater :: (Config.t() -> Config.t())) :: State.t()
    def update_config(t, updater), do: update_in(t.config, updater)

    @spec set_last_durable_txn_id(t :: State.t(), Raft.transaction_id()) :: State.t()
    def set_last_durable_txn_id(t, txn_id), do: put_in(t.last_durable_txn_id, txn_id)
  end
end
