defmodule Bedrock.Service.StorageController do
  @moduledoc """
  Responsible for managing the lifecycle of the Storage services on this node,
  for a given cluster.
  """
  use Bedrock.Cluster, :types

  alias Bedrock.Service.Storage
  alias Bedrock.Service.Controller

  @type t :: Controller.server()

  @spec storage(storage_controller :: t()) :: {:ok, [Storage.t()]} | {:error, term()}
  defdelegate storage(storage_controller), to: Controller, as: :workers

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Supervisor
end
