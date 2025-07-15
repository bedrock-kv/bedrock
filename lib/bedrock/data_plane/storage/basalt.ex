defmodule Bedrock.DataPlane.Storage.Basalt do
  use Bedrock.Service.WorkerBehaviour, kind: :storage

  @doc false
  @spec child_spec(
          opts :: [
            otp_name: atom(),
            foreman: pid(),
            id: String.t(),
            path: Path.t()
          ]
        ) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: __MODULE__.Server
end
