defmodule Bedrock.DataPlane.Materializer.Basalt do
  @moduledoc false

  use Bedrock.Service.WorkerBehaviour, kind: :materializer

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
