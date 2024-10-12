defmodule Bedrock.ControlPlane.Coordinator.State do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Config
  alias Bedrock.Raft

  @type t :: %__MODULE__{
          cluster: module(),
          am_i_the_leader: boolean(),
          controller: :unavailable | ClusterController.ref(),
          controller_otp_name: atom(),
          my_node: node(),
          otp_name: atom(),
          raft: Raft.t(),
          supervisor_otp_name: atom(),
          last_durable_txn_id: Raft.transaction_id(),
          config: Config.t(),
          waiting_list: %{Raft.transaction_id() => pid()}
        }
  defstruct cluster: nil,
            am_i_the_leader: false,
            controller: :unavailable,
            controller_otp_name: nil,
            my_node: nil,
            otp_name: nil,
            raft: nil,
            supervisor_otp_name: nil,
            last_durable_txn_id: nil,
            config: nil,
            waiting_list: %{}

  @spec update_controller(t :: t(), new_controller :: ClusterController.ref()) :: t()
  def update_controller(t, new_controller), do: put_in(t.controller, new_controller)

  @spec update_am_i_the_leader(t :: t(), am_i_the_leader :: boolean()) :: t()
  def update_am_i_the_leader(t, am_i_the_leader), do: put_in(t.am_i_the_leader, am_i_the_leader)

  @spec update_raft(t(), Raft.t()) :: t()
  def update_raft(t, raft), do: put_in(t.raft, raft)

  @spec update_config(t(), Config.t()) :: t()
  def update_config(t, config), do: put_in(t.config, config)

  @spec update_last_durable_txn_id(t(), Raft.transaction_id()) :: t()
  def update_last_durable_txn_id(t, txn_id), do: put_in(t.last_durable_txn_id, txn_id)
end
