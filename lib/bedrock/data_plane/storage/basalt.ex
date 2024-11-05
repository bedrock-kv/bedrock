defmodule Bedrock.DataPlane.Storage.Basalt do
  use Bedrock.Service.WorkerBehaviour, kind: :storage

  @doc false
  @spec child_spec(opts :: keyword()) :: map()
  defdelegate child_spec(opts), to: __MODULE__.Server
end
