defmodule Bedrock.Service.TransactionLog do
  use Bedrock.Cluster, :types

  @type t :: GenServer.name()
  @type id :: binary()

  @doc """
  """
  @spec workers(t()) :: {:ok, [Bedrock.Service.Worker.t()]} | {:error, term()}
  defdelegate workers(t), to: Bedrock.Service.Controller

  @spec wait_for_healthy(t(), :infinity | non_neg_integer()) :: :ok | {:error, any()}
  def wait_for_healthy(cluster, timeout) do
    cluster.otp_name(:transaction_log_controller)
    |> Bedrock.Service.Controller.wait_for_healthy(timeout)
  end

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Supervisor
end
