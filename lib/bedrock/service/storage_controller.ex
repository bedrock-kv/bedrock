defmodule Bedrock.Service.StorageController do
  @moduledoc """
  Responsible for managing the lifecycle of the Storage services on this node,
  for a given cluster.
  """
  use Bedrock.Service.Controller,
    kind: :storage,
    worker: Bedrock.DataPlane.Storage,
    default_worker: Bedrock.DataPlane.Storage.Basalt
end
