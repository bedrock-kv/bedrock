defmodule Bedrock.ControlPlane.ClusterController.State do
  @moduledoc false

  alias Bedrock.ControlPlane.ClusterController.NodeTracking
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type t :: %__MODULE__{
          epoch: Bedrock.epoch(),
          otp_name: atom(),
          cluster: module(),
          config: Config.t() | nil,
          coordinator: pid(),
          node_tracking: NodeTracking.t(),
          timer_ref: reference() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          last_transaction_layout_id: TransactionSystemLayout.id()
        }
  defstruct epoch: nil,
            otp_name: nil,
            cluster: nil,
            config: nil,
            coordinator: nil,
            node_tracking: nil,
            service_directory: nil,
            timer_ref: nil,
            transaction_system_layout: nil,
            last_transaction_layout_id: 0
end
