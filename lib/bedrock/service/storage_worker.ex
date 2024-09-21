defmodule Bedrock.Service.StorageWorker do
  use Bedrock, :types
  use Bedrock.Cluster, :types

  @type name :: Bedrock.Service.Worker.t()
  @type key_range :: {min_inclusive :: key(), max_exclusive :: key()}
  @type fact_name ::
          Bedrock.Service.Worker.fact_name()
          | :durable_version
          | :key_range
          | :n_objects
          | :path
          | :size_in_bytes
          | :utilization

  defmacro __using__(:types) do
    quote do
      @type name :: Bedrock.Service.StorageWorker.name()
      @type key_range :: Bedrock.Service.StorageWorker.key_range()
      @type fact_name :: Bedrock.Service.StorageWorker.fact_name()
    end
  end

  @doc """
  Returns the value for the given key/version.
  """
  @spec fetch(name(), key(), version(), timeout_in_ms()) ::
          {:ok, value()}
          | {:error,
             :timeout
             | :not_found
             | :transaction_too_old
             | :transaction_too_new
             | :unavailable}
  def fetch(worker, key, version, timeout \\ 5_000) when is_binary(key) do
    GenServer.call(worker, {:fetch, key, version, [timeout: timeout]})
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :unavailable}
  end

  @doc """
  Ask the storage engine for various facts about itself.
  """
  @spec info(name(), [fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(name(), [fact_name()], timeout_in_ms()) :: {:ok, keyword()} | {:error, term()}
  defdelegate info(worker, fact_names, timeout \\ 5_000),
    to: Bedrock.Service.Worker
end
