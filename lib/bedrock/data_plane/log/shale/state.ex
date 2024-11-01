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
          pending_transactions: %{Bedrock.version() => {Transaction.t(), pid()}}
        }
  defstruct [
    :mode,
    :id,
    :otp_name,
    :foreman,
    :epoch,
    :controller,
    :log,
    :oldest_version,
    :last_version,
    :pending_transactions
  ]
end
