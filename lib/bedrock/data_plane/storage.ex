defmodule Bedrock.DataPlane.Storage do
  alias Bedrock.Service.Worker

  import Bedrock.Internal.GenServer.Calls

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

  @type recovery_info :: %{
          kind: :storage,
          durable_version: Bedrock.version(),
          oldest_durable_version: Bedrock.version()
        }

  @spec recovery_info :: [fact_name()]
  def recovery_info, do: [:kind, :durable_version, :oldest_durable_version]

  @doc """
  Returns the value for the given key/version.
  """
  @spec fetch(
          storage :: ref(),
          Bedrock.key(),
          Bedrock.version(),
          opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]
        ) ::
          {:ok, Bedrock.value()}
          | {:error,
             :timeout
             | :not_found
             | :tx_too_old
             | :tx_too_new
             | :unavailable}
  def fetch(storage, key, version, opts \\ []) when is_binary(key),
    do: call(storage, {:fetch, key, version, opts}, opts[:timeout_in_ms] || :infinity)

  @doc """
  Request that the storage service lock itself and stop pulling new transactions
  from the logs. This mechanism is used by a newly elected cluster director
  to prevent new transactions from being accepted while it is establishing
  its authority.

  In order for the lock to succeed, the given epoch needs to be greater than
  the current epoch.
  """
  @spec lock_for_recovery(storage :: ref(), Bedrock.epoch()) ::
          {:ok, pid(), recovery_info :: keyword()} | {:error, :newer_epoch_exists}
  defdelegate lock_for_recovery(storage, epoch), to: Worker

  @doc """
  Ask the storage storage for various facts about itself.
  """
  @spec info(storage :: ref(), [fact_name()], opts :: keyword()) ::
          {:ok, keyword()} | {:error, term()}
  defdelegate info(storage, fact_names, opts \\ []), to: Worker
end
