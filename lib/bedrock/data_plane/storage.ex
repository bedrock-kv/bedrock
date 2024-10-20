defmodule Bedrock.DataPlane.Storage do
  alias Bedrock.Service.Worker

  @type ref :: Worker.ref()
  @type id :: Worker.id()
  @type key_range :: Bedrock.key_range()
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
  @spec fetch(storage :: ref(), Bedrock.key(), Bedrock.version(), Bedrock.timeout_in_ms()) ::
          {:ok, Bedrock.value()}
          | {:error,
             :timeout
             | :not_found
             | :tx_too_old
             | :tx_too_new
             | :unavailable}
  def fetch(storage, key, version, timeout \\ 5_000) when is_binary(key) do
    GenServer.call(storage, {:fetch, key, version, [timeout: timeout]})
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @doc """
  Ask the storage storage for various facts about itself.
  """
  @spec info(storage :: ref(), [fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(storage :: ref(), [fact_name()], Bedrock.timeout_in_ms()) ::
          {:ok, keyword()} | {:error, term()}
  defdelegate info(storage, fact_names, timeout \\ 5_000), to: Worker
end
