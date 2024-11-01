defmodule Bedrock.DataPlane.Log do
  @moduledoc """
  """

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Worker

  use Bedrock.Internal.GenServerApi

  @type ref :: Worker.ref()
  @type id :: Worker.id()
  @type health :: Worker.health()
  @type fact_name ::
          Worker.fact_name()
          | :last_version
          | :oldest_version
          | :minimum_durable_version

  @type recovery_info :: %{
          kind: :log,
          last_version: Bedrock.version(),
          oldest_version: Bedrock.version(),
          minimum_durable_version: Bedrock.version() | :unavailable
        }

  @spec recovery_info :: [fact_name()]
  def recovery_info, do: [:kind, :last_version, :oldest_version, :minimum_durable_version]

  @doc """
  Apply a new mutation to the log. The previous transaction id is given as a
  check to ensure strict ordering. If the previous transaction id is not the
  latest transaction, the mutation will be rejected.
  """
  @spec push(log :: ref(), Transaction.t(), last_commit_version :: Bedrock.version()) ::
          :ok | {:error, :tx_out_of_order | :locked | :unavailable}
  def push(log, transaction, last_commit_version) do
    GenServer.call(log, {:push, transaction, last_commit_version})
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @spec pull(
          log :: ref(),
          last_version :: Bedrock.version(),
          opts :: [
            limit: pos_integer(),
            last_version: Bedrock.version(),
            recovery: boolean(),
            subscriber: {id :: String.t(), last_durable_version :: Bedrock.version()}
          ]
        ) ::
          {:ok, [Transaction.t()]}
          | {:error, :not_ready}
          | {:error, :version_too_new}
          | {:error, :version_too_old}
          | {:error, :version_not_found}
          | {:error, :unavailable}
  def pull(log, last_version, opts),
    do: call(log, {:pull, last_version, opts}, :infinity)

  @doc """
  Request that the transaction log worker lock itself and stop accepting new
  transactions. This mechanism is used by a newly elected cluster controller
  to prevent new transactions from being accepted while it is establishing
  its authority.

  In order for the lock to succeed, the given epoch needs to be greater than
  the current epoch.
  """
  @spec lock_for_recovery(log :: ref(), Bedrock.epoch()) ::
          {:ok, pid(), recovery_info :: keyword()} | {:error, :newer_epoch_exists}
  defdelegate lock_for_recovery(storage, epoch), to: Worker

  @doc """
  Initiates a recovery process from the given `source_log` spanning the
  transactions specified by the `version_vector`.

  The function ensures that the transaction log is consistent with the
  `source_log` by pulling from that log and applying the transactions.

  ## Parameters:

    - `log`: Reference to the target log where recovery should be applied.
    - `source_log`: Reference to the source log from which transactions are
      recovered. nil is sent for the initial recovery, since there is no
      source log.
    - `version_vector`: The version vector indicating the starting and ending
      point of recovery.

  ## Return Values:

    - `:ok`: Recovery was successful.
    - `{:error, :unavailable}`: The log is unavailable, and recovery cannot be
      performed.
  """
  @spec recover_from(log :: ref(), source_log :: ref() | nil, Bedrock.version_vector()) ::
          :ok | {:error, :unavailable}
  def recover_from(log, source_log, version_vector),
    do: call(log, {:recover_from, source_log, version_vector}, :infinity)

  @doc """
  Ask the transaction log worker for various facts about itself.
  """
  @spec info(storage :: ref(), [fact_name()], opts :: keyword()) ::
          {:ok, keyword()} | {:error, term()}
  defdelegate info(storage, fact_names, opts \\ []), to: Worker
end
