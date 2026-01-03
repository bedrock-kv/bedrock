defmodule Bedrock.DataPlane.Storage.Basalt do
  @moduledoc false

  use Bedrock.Service.WorkerBehaviour, kind: :storage

  @doc false
  @spec child_spec(
          opts :: [
            otp_name: atom(),
            foreman: Bedrock.Service.Foreman.ref(),
            id: Bedrock.service_id(),
            path: Path.t()
          ]
        ) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: __MODULE__.Server
end
