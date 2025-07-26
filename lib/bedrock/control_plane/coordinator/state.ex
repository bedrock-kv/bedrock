defmodule Bedrock.ControlPlane.Coordinator.State do
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Config
  alias Bedrock.Raft

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
          waiting_list: %{Raft.transaction_id() => pid()}
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
            waiting_list: %{}

  defmodule Changes do
    alias Bedrock.ControlPlane.Coordinator.State

    @spec put_epoch(t :: State.t(), epoch :: Bedrock.epoch()) :: State.t()
    def put_epoch(t, epoch), do: %{t | epoch: epoch}

    @spec put_director(t :: State.t(), new_director :: Director.ref() | :unavailable) ::
            State.t()
    def put_director(t, new_director),
      do: %{t | director: new_director}

    @spec put_leader_node(t :: State.t(), leader_node :: node() | :undecided) :: State.t()
    def put_leader_node(t, leader_node), do: %{t | leader_node: leader_node}

    @spec set_raft(t :: State.t(), Raft.t()) :: State.t()
    def set_raft(t, raft), do: %{t | raft: raft}

    @spec update_raft(t :: State.t(), updater :: (Raft.t() -> Raft.t())) :: State.t()
    def update_raft(t, updater), do: %{t | raft: updater.(t.raft)}

    @spec put_config(t :: State.t(), Config.t()) :: State.t()
    def put_config(t, config), do: %{t | config: config}

    @spec update_config(t :: State.t(), updater :: (Config.t() -> Config.t())) :: State.t()
    def update_config(t, updater), do: %{t | config: updater.(t.config)}

    @spec put_last_durable_txn_id(t :: State.t(), Raft.transaction_id()) :: State.t()
    def put_last_durable_txn_id(t, last_durable_txn_id),
      do: %{t | last_durable_txn_id: last_durable_txn_id}
  end
end
