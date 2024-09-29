defmodule Bedrock.DataPlane.StorageController do
  @moduledoc """
  Responsible for managing the lifecycle of the Storage services on this node,
  for a given cluster.
  """
  use Bedrock.Service.Controller,
    kind: :storage,
    default_worker: Bedrock.DataPlane.Storage.Basalt
end
