defmodule Bedrock.DataPlane.Log do
  @moduledoc """
  Transaction log service for the data plane.

  Logs store committed transactions in order and support replication across nodes.
  """
  use Bedrock.Internal.GenServerApi

  # EncodedTransaction removed - using Transaction binary format
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Worker

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

  @doc """
  Returns information needed for recovery processes. This includes the kind of
  entity (:log) and various version markers indicating the state and durability
  of the log.

  ## Return Values:

    - A list containing:
      - `:kind`: Identifies the entity as a log.
      - `:last_version`: The latest version present in the log.
      - `:oldest_version`: The earliest version still available in the log.
      - `:minimum_durable_version`: The lowest version that is guaranteed to
        be durable. This could be a specific version or `:unavailable` if
        not determinable.

  Useful in scenarios where system recovery needs to consider the current
  state of log versions, ensuring consistency and order.
  """
  @spec recovery_info :: [fact_name()]
  def recovery_info, do: [:kind, :last_version, :oldest_version, :minimum_durable_version]

  @doc """
  Apply a new transaction to the log. The previous transaction version is given
  as a check to ensure strict ordering. If the previous transaction version is
  lower than the latest transaction, the transaction will be rejected. If it is
  greater, then the transaction will be queued for later application.

  This call will not return until the transaction has been made durable on the
  log, ensuring that the transactions that precede it are also durable.
  """
  @spec push(
          log_ref :: ref(),
          transaction :: Transaction.encoded(),
          last_commit_version :: Bedrock.version()
        ) ::
          :ok | {:error, :tx_out_of_order | :locked | :unavailable}
  def push(log, transaction, last_commit_version), do: call(log, {:push, transaction, last_commit_version}, :infinity)

  @doc """
  Pull transactions from the log starting from a given version. Options allow
  specifying the maximum number of transactions to return, the last version
  considered valid, whether the operation is recovery-related, subscriber
  details to maintain state, and a timeout for the operation.

  Returns a list of transactions or an error indicating why the pull failed.

  ## Parameters:

    - `log`: Reference to the log from which transactions are to be pulled.
    - `start_after`: The version after which transactions are to be pulled.
    - `opts`: Options to tailor the behavior of the pull operation.
      - `limit`: Maximum number of transactions to return. This may be
        additionally limited by the log's configuration.
      - `last_version`: The last valid version for pulling transactions
        (inclusive).
      - `recovery`: Indicates if this pull is part of a recovery operation.
      - `subscriber`: A tuple containing an ID and the last durable version.
      - `timeout_in_ms`: Timeout for the operation in milliseconds.

  ## Return Values:

    - `{:ok, [Transaction.encoded()]}`: A successful pull with a list of encoded transactions.
    - `{:error, :not_ready}`: Log is not ready for pulling.
    - `{:error, :not_locked}`: Log is not locked for pulling transactions.
    - `{:error, :invalid_from_version}`: The provided `from_version` is invalid.
    - `{:error, :invalid_last_version}`: The specified `last_version` is invalid.
    - `{:error, :version_too_new}`: The version specified is too recent.
    - `{:error, :version_too_old}`: The version specified is too old.
    - `{:error, :version_not_found}`: The version cannot be found.
    - `{:error, :unavailable}`: Log is unavailable for operation.
  """
  @spec pull(
          log_ref :: ref(),
          start_after_version :: Bedrock.version(),
          opts :: [
            limit: pos_integer(),
            last_version: Bedrock.version(),
            recovery: boolean(),
            subscriber: {subscriber_id :: String.t(), last_durable_version :: Bedrock.version()},
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) ::
          {:ok, transactions :: [Transaction.encoded()]} | pull_errors()
  @type pull_errors ::
          {:error, :not_ready}
          | {:error, :not_locked}
          | {:error, :invalid_from_version}
          | {:error, :invalid_last_version}
          | {:error, :version_too_new}
          | {:error, :version_too_old}
          | {:error, :version_not_found}
          | {:error, :unavailable}
  @type pull_error :: pull_errors()
  def pull(log, start_after, opts), do: call(log, {:pull, start_after, opts}, opts[:timeout_in_ms] || :infinity)

  @doc """
  The initial transaction that is applied to a new log if the current version
  is set to 0 during a recovery. It is an explicit a directive to clear the
  entire key range.
  """
  @spec initial_transaction :: Transaction.encoded()
  def initial_transaction do
    # Create an empty transaction with no mutations
    encoded = Transaction.encode(%{mutations: []})
    # Add zero version as commit version
    zero_version = Version.from_integer(0)
    {:ok, with_version} = Transaction.add_commit_version(encoded, zero_version)
    with_version
  end

  @doc """
  Request that the transaction log worker lock itself and stop accepting new
  transactions. This mechanism is used by a newly elected cluster director
  to prevent new transactions from being accepted while it is establishing
  its authority.

  In order for the lock to succeed, the given epoch needs to be greater than
  the current epoch.
  """
  @spec lock_for_recovery(log :: ref(), Bedrock.epoch()) ::
          {:ok, pid(),
           recovery_info :: [
             kind: :log,
             last_version: Bedrock.version(),
             oldest_version: Bedrock.version(),
             minimum_durable_version: Bedrock.version() | :unavailable
           ]}
          | {:error, :newer_epoch_exists}
  defdelegate lock_for_recovery(storage, epoch), to: Worker

  @doc """
  Initiates a recovery process from the given source log(s) spanning the
  transactions specified by the given first/last versions. If the first version
  is 0, a special _initial transaction_ is applied to the log. This transaction
  is defined by `initial_transaction/0`.

  The function ensures that the transaction log is consistent with the
  source logs by pulling from them and applying the transactions.

  ## Parameters:

    - `log`: Reference to the target log where recovery should be applied.
    - `source_logs`: List of source log references from which transactions are
      recovered, or a single ref for backward compatibility. Empty list or nil
      is sent for initial recovery when there are no source logs.
    - `first_version`: The starting point of the recovery. Transactions _after_
      this version are applied.
    - `last_version`: The ending point of the recovery. Transactions up to and
      including this version are applied.

  ## Multi-Source Recovery (Consistent Hashing)

  When multiple source logs are provided, the log pulls transactions from all
  sources and uses the shard index embedded in each transaction to filter for
  mutations belonging to shards this log serves (determined by ShardRouter).
  This supports the consistent hashing model where shard→log mapping is computed
  rather than stored.

  ## Return Values:

    - `{:ok, pid}`: Recovery was successful, returns the log's PID for subsequent operations.
    - `{:error, :unavailable}`: The log is unavailable, and recovery cannot be
      performed.
  """
  @spec recover_from(
          log :: ref(),
          source_logs :: [ref()] | ref() | nil,
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, pid()} | {:error, :unavailable}
  def recover_from(log, source_logs, first_version, last_version),
    do: call(log, {:recover_from, normalize_source_logs(source_logs), first_version, last_version}, :infinity)

  # Normalize source_logs to always be a list for consistent handling
  defp normalize_source_logs(nil), do: []
  defp normalize_source_logs([]), do: []
  defp normalize_source_logs(logs) when is_list(logs), do: logs
  defp normalize_source_logs(single_log), do: [single_log]

  @doc """
  Ask the transaction log worker for various facts about itself.
  """
  @spec info(storage :: ref(), [fact_name()], opts :: keyword()) ::
          {:ok, %{fact_name() => :log | Bedrock.version() | atom() | pid()}}
          | {:error, :unavailable | :timeout}
  defdelegate info(storage, fact_names, opts \\ []), to: Worker
end
