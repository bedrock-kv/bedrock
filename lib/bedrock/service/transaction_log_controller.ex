defmodule Bedrock.Service.LogController do
  @moduledoc """
  Responsible for managing the lifecycle of the Log services on this
  node, for a given cluster.
  """
  use Bedrock.Service.Controller,
    kind: :transaction_log,
    worker: Bedrock.DataPlane.Log,
    default_worker: Bedrock.DataPlane.Log.Limestone
end
