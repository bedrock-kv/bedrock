defmodule Bedrock.Service.Storage do
  use Bedrock.Cluster, :types

  alias Bedrock.Service.Worker

  @type t :: GenServer.server()

  @spec workers(t()) :: {:ok, [Worker.worker()]} | {:error, term()}
  defdelegate workers(t), to: Bedrock.Service.Controller

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Supervisor
end
