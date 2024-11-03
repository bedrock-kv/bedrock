defmodule Bedrock.DataPlane.Log.Shale.State do
  alias Bedrock.Service.Worker
  alias Bedrock.Service.Foreman
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ControlPlane.ClusterController

  @type mode :: :waiting | :locked | :running

  @type t :: %__MODULE__{
          cluster: module(),
          controller: ClusterController.ref(),
          epoch: Bedrock.epoch(),
          id: Worker.id(),
          foreman: Foreman.ref(),
          last_version: Bedrock.version(),
          log: :ets.table(),
          mode: mode(),
          oldest_version: Bedrock.version(),
          otp_name: Worker.otp_name(),
          params: %{
            default_pull_limit: pos_integer(),
            max_pull_limit: pos_integer()
          },
          pending_transactions: %{Bedrock.version() => {Transaction.t(), pid()}}
        }
  defstruct cluster: nil,
            controller: nil,
            epoch: nil,
            foreman: nil,
            id: nil,
            last_version: nil,
            log: nil,
            mode: :waiting,
            oldest_version: nil,
            otp_name: nil,
            pending_transactions: %{},
            params: %{
              default_pull_limit: 100,
              max_pull_limit: 500
            }
end
