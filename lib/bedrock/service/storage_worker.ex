defmodule Bedrock.Service.StorageWorker do
  use Bedrock, :types
  use Bedrock.Cluster, :types

  alias Bedrock.Service.Worker

  @type name :: Worker.t()
  @type key_range :: {min_inclusive :: key(), max_exclusive :: key()}
  @type fact_name ::
          Worker.fact_name()
          | :durable_version
          | :key_range
          | :n_objects
          | :path
          | :size_in_bytes
          | :utilization

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
  Ask the storage worker for various facts about itself.
  """
  @spec info(name(), [fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(name(), [fact_name()], timeout_in_ms()) :: {:ok, keyword()} | {:error, term()}
  defdelegate info(worker, fact_names, timeout \\ 5_000),
    to: Worker
end
