defmodule Bedrock.Service.TransactionLogController do
  @moduledoc """
  Responsible for managing the lifecycle of the TransactionLog services on this
  node, for a given cluster.
  """

  alias Bedrock.Service.Controller
  alias Bedrock.Service.TransactionLog

  @type t :: Controller.server()
  @type id :: binary()

  @doc """
  """
  @spec transaction_logs(controller :: t()) :: {:ok, [TransactionLog.t()]} | {:error, term()}
  defdelegate transaction_logs(controller), to: Controller, as: :workers

  @spec wait_for_healthy(controller :: t(), :infinity | non_neg_integer()) ::
          :ok | {:error, any()}
  defdelegate wait_for_healthy(controller, timeout), to: Controller

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Supervisor
end
