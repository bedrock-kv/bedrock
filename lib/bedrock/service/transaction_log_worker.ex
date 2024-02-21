defmodule Bedrock.Service.TransactionLogWorker do
  use Bedrock.Cluster, :types

  @type name :: Bedrock.Service.Worker.name()
  @type fact_name :: Bedrock.Service.Worker.fact_name() | :path
  @type timeout_in_ms :: :infinity | non_neg_integer()

  defmacro __using__(:types) do
    quote do
      @type name :: Bedrock.Service.TransactionLogWorker.name()
      @type fact_name :: Bedrock.Service.TransactionLogWorker.fact_name()
      @type timeout_in_ms :: Bedrock.Service.TransactionLogWorker.timeout_in_ms()
    end
  end

  @doc """
  Ask the transaction log engine for various facts about itself.
  """
  @spec info(name(), [fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(name(), [fact_name()], timeout_in_ms()) :: {:ok, keyword()} | {:error, term()}
  defdelegate info(worker, fact_names, timeout \\ 5_000),
    to: Bedrock.Service.Worker
end
