defmodule Bedrock.Service.TransactionLogWorker do
  use Bedrock, :types
  use Bedrock.Cluster, :types

  @type name :: Bedrock.Service.Worker.name()
  @type fact_name :: Bedrock.Service.Worker.fact_name() | :path

  defmacro __using__(:types) do
    quote do
      @type name :: Bedrock.Service.TransactionLogWorker.name()
      @type fact_name :: Bedrock.Service.TransactionLogWorker.fact_name()
      @type timeout_in_ms :: Bedrock.Service.TransactionLogWorker.timeout_in_ms()
    end
  end

  @doc """
  Request that the transaction log worker lock itself. After receiving this
  message, the worker will cease to accept new transactions. The calling process
  is passed to the log worker.
  """
  @spec request_lock(name(), controller :: pid(), epoch()) :: :ok
  def request_lock(worker, controller, epoch),
    do: GenServer.cast(worker, {:request_lock, controller, epoch})

  @doc """
  Ask the transaction log engine for various facts about itself.
  """
  @spec info(name(), [fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(name(), [fact_name()], timeout_in_ms()) :: {:ok, keyword()} | {:error, term()}
  defdelegate info(worker, fact_names, timeout \\ 5_000),
    to: Bedrock.Service.Worker
end
