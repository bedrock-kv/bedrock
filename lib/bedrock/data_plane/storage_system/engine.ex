defmodule Bedrock.DataPlane.StorageSystem.Engine do
  use Bedrock, :types
  use Bedrock.Cluster, :types

  @type t :: Bedrock.Engine.t()
  @type key_range :: {min_inclusive :: key(), max_exclusive :: key()}

  @type timeout_in_ms :: :infinity | non_neg_integer()

  @type basic_fact_name ::
          :durable_version
          | :key_range
          | :kind
          | :id
          | :n_objects
          | :otp_name
          | :path
          | :size_in_bytes
          | :supported_info
          | :utilization

  defmacro __using__(:types) do
    quote do
      alias Bedrock.DataPlane.StorageSystem.Engine

      @type key_range :: StorageSystem.Engine.key_range()
    end
  end

  @doc """
  Returns the value for the given key/version.
  """
  @spec get(t(), key(), version(), timeout_in_ms()) ::
          {:ok, value()}
          | {:error,
             :timeout
             | :not_found
             | :transaction_too_old
             | :transaction_too_new
             | :engine_does_not_exist}
  def get(storage_engine, key, version, timeout \\ 5_000) when is_binary(key) do
    GenServer.call(storage_engine, {:get, key, version, [timeout: timeout]})
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :engine_does_not_exist}
  end

  @doc """
  Ask the storage engine for various facts about itself.
  """
  @spec info(t(), [basic_fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(t(), [basic_fact_name()], timeout_in_ms()) :: {:ok, keyword()} | {:error, term()}
  defdelegate info(storage_engine, fact_names, timeout \\ 5_000), to: Bedrock.Engine
end
