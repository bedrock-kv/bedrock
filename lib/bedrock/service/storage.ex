defmodule Bedrock.Service.Storage do
  use Bedrock.Cluster, :types

  @type t :: GenServer.name()

  @spec workers(t()) :: {:ok, [Bedrock.Service.Worker.t()]} | {:error, term()}
  defdelegate workers(t), to: Bedrock.Service.Controller

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Supervisor
end
