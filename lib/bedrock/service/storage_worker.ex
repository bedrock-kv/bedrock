defmodule Bedrock.Service.Storage do
  use Bedrock, :types
  use Bedrock.Cluster, :types

  alias Bedrock.Service.Worker

  @type t :: Worker.t()
  @type id :: Worker.id()
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
  @spec fetch(storage :: t(), key(), version(), timeout_in_ms()) ::
          {:ok, value()}
          | {:error,
             :timeout
             | :not_found
             | :transaction_too_old
             | :transaction_too_new
             | :unavailable}
  def fetch(storage, key, version, timeout \\ 5_000) when is_binary(key) do
    GenServer.call(storage, {:fetch, key, version, [timeout: timeout]})
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :unavailable}
  end

  @doc """
  Ask the storage storage for various facts about itself.
  """
  @spec info(storage :: t(), [fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(storage :: t(), [fact_name()], timeout_in_ms()) ::
          {:ok, keyword()} | {:error, term()}
  defdelegate info(storage, fact_names, timeout \\ 5_000),
    to: Worker
end
