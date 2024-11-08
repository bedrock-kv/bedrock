defmodule Bedrock.DataPlane.Log.Shale.State do
  alias Bedrock.Service.Worker
  alias Bedrock.Service.Foreman
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ControlPlane.Director

  @type mode :: :locked | :running

  @type t :: %__MODULE__{
          cluster: module(),
          director: Director.ref(),
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
          pending_transactions: %{Bedrock.version() => {Transaction.t(), pid()}},
          waiting_pullers: %{
            Bedrock.version() => [{Bedrock.timestamp_in_ms(), pid(), opts :: keyword()}]
          }
        }
  defstruct cluster: nil,
            director: nil,
            epoch: nil,
            foreman: nil,
            id: nil,
            last_version: nil,
            log: nil,
            mode: :locked,
            oldest_version: nil,
            otp_name: nil,
            pending_transactions: %{},
            waiting_pullers: %{},
            params: %{
              default_pull_limit: 100,
              max_pull_limit: 500
            }
end
