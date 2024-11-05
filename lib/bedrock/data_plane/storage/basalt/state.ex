defmodule Bedrock.DataPlane.Storage.Basalt.State do
  alias Bedrock.DataPlane.Storage.Basalt.Database
  alias Bedrock.Service.Worker

  @type t :: %__MODULE__{
          otp_name: atom(),
          path: Path.t(),
          epoch: Bedrock.epoch() | nil,
          foreman: pid() | nil,
          id: Worker.id(),
          database: Database.t()
        }
  defstruct otp_name: nil,
            path: nil,
            epoch: nil,
            foreman: nil,
            id: nil,
            database: nil
end
