defmodule Bedrock.DataPlane.Log.Shale.State do
  alias Bedrock.Service.Worker
  alias Bedrock.Service.Foreman
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ControlPlane.ClusterController

  @type mode :: :waiting | :locked | :running

  @type t :: %__MODULE__{
          id: Worker.id(),
          otp_name: Worker.otp_name(),
          foreman: Foreman.ref(),
          log: :ets.table(),
          epoch: Bedrock.epoch(),
          controller: ClusterController.ref(),
          mode: mode(),
          oldest_version: Bedrock.version(),
          last_version: Bedrock.version(),
          pending_transactions: %{Bedrock.version() => {Transaction.t(), pid()}},
          params: %{
            default_pull_limit: pos_integer(),
            max_pull_limit: pos_integer()
          }
        }
  defstruct mode: :waiting,
            id: nil,
            otp_name: nil,
            foreman: nil,
            epoch: nil,
            controller: nil,
            log: nil,
            oldest_version: nil,
            last_version: nil,
            pending_transactions: %{},
            params: %{
              default_pull_limit: 100,
              max_pull_limit: 500
            }
end
