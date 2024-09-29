defmodule Bedrock.Service.TransactionLogController do
  @moduledoc """
  Responsible for managing the lifecycle of the TransactionLog services on this
  node, for a given cluster.
  """
  use Bedrock.Service.Controller,
    kind: :transaction_log,
    default_worker: Bedrock.DataPlane.TransactionLog.Limestone
end
