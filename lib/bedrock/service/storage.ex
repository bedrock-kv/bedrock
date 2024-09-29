defmodule Bedrock.Service.Storage do
  use Bedrock.Cluster, :types

  alias Bedrock.Service.Worker
  alias Bedrock.Service.Controller

  @type t :: Worker.t()

  @spec storage(storage_controller :: t()) :: {:ok, [Worker.t()]} | {:error, term()}
  defdelegate storage(storage_controller), to: Controller, as: :workers

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Supervisor
end
