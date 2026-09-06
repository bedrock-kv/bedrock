defmodule Bedrock.DataPlane.Log do
  @moduledoc """
  Transaction log service for the data plane.

  Logs store committed transactions in order and support replication across nodes.
  """
  use Bedrock.Internal.GenServerApi

  # EncodedTransaction removed - using Transaction binary format
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.RecoveryAuthority
  alias Bedrock.Service.Worker

  @type ref :: Worker.ref()
  @type id :: Worker.id()
  @type health :: Worker.health()
  @type wal_limit_error ::
          {:recovery_required,
           {:wal_limit_exceeded,
            %{
              commit_version: Bedrock.version(),
              min_durable_version: Bedrock.version(),
              last_version: Bedrock.version(),
              lag_us: pos_integer(),
              limit_us: non_neg_integer()
            }}}
  @type fact_name ::
          Worker.fact_name()
          | :last_version
          | :available_after
          | :oldest_version
          | :minimum_durable_version

  @type recovery_info :: %{
          kind: :log,
          last_version: Bedrock.version(),
          available_after: Bedrock.version(),
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
      - `:available_after`: The exclusive cursor after which every retained
        transaction is available.
      - `:oldest_version`: Informational first retained transaction version.
      - `:minimum_durable_version`: The object-storage watermark through which
        data is durable. This could be a specific version or `:unavailable`
        after restart.

  Useful in scenarios where system recovery needs to consider the current
  state of log versions, ensuring consistency and order.
  """
  @spec recovery_info :: [fact_name()]
  def recovery_info, do: [:kind, :last_version, :available_after, :oldest_version, :minimum_durable_version]

  @doc """
  Apply a new transaction to the log. The previous transaction version is given
  as a check to ensure strict ordering. If the previous transaction version is
  lower than the latest transaction, the transaction will be rejected. If it is
  greater, then the transaction will be queued for later application.

  This call will not return until the transaction has been made durable on the
  log, ensuring that the transactions that precede it are also durable.

  If the optional WAL lag safety limit is exceeded, the call returns
  `{:error, {:recovery_required, {:wal_limit_exceeded, details}}}`. This is
  fatal to the current transaction-system epoch: the caller must not retry or
  continue the assigned version chain without coordinated recovery.

  ## Options

    - `known_committed_version`: The highest version known to be committed
      (fully replicated) at the time this push was prepared, piggybacked from
      the sequencer. Downstream durability sinks (chunk flushes) gate on it
      so that nothing not known-committed ever becomes durable.
  """
  @spec push(
          log_ref :: ref(),
          authority :: RecoveryAuthority.input(),
          transaction :: Transaction.encoded(),
          last_commit_version :: Bedrock.version(),
          opts :: [known_committed_version: Bedrock.version() | nil]
        ) ::
          :ok | {:error, :tx_out_of_order | :locked | :unavailable | wal_limit_error()}
  @spec push(ref(), Transaction.encoded(), Bedrock.version()) :: {:error, :invalid_recovery_authority}
  def push(_log, _transaction, _last_commit_version), do: {:error, :invalid_recovery_authority}

  @spec push(ref(), Transaction.encoded(), Bedrock.version(), keyword()) :: {:error, :invalid_recovery_authority}
  def push(_log, _transaction, _last_commit_version, _opts), do: {:error, :invalid_recovery_authority}

  def push(log, authority, transaction, last_commit_version, opts) do
    call(log, {:push, authority, transaction, last_commit_version, opts[:known_committed_version]}, :infinity)
  end

  @doc """
  Pull transactions from the log starting from a given version. Options allow
  specifying the maximum number of transactions to return, the last version
  considered valid, whether the operation is recovery-related, and a timeout
  for the operation.

  Recovery is this call's only remaining consumer: materializers stream
  their shard from the log's Demux (`get_shard_server/2`), not from the
  log itself.

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
      - `timeout_in_ms`: Timeout for the operation in milliseconds.

  ## Return Values:

    - `{:ok, [Transaction.encoded()]}`: A successful pull with a list of encoded transactions.
    - `{:error, :not_ready}`: Log is not ready for pulling.
    - `{:error, :not_locked}`: Log is not locked for pulling transactions.
    - `{:error, :invalid_from_version}`: The provided `from_version` is invalid.
    - `{:error, :invalid_last_version}`: The specified `last_version` is invalid.
    - `{:error, :version_too_new}`: The version specified is too recent.
    - `{:error, {:version_too_old, floor}}`: The version specified is below
      the WAL's trim floor; `floor` is the exclusive `available_after` cursor.
      Consumers below it should catch up from object storage chunks.
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
            recovery_authority: RecoveryAuthority.input(),
            willing_to_wait_in_ms: Bedrock.timeout_in_ms(),
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
          | {:error, {:version_too_old, floor :: Bedrock.version()}}
          | {:error, :version_not_found}
          | {:error, :unavailable}
          | {:error, :invalid_recovery_authority}
  @type pull_error :: pull_errors()
  def pull(log, start_after, opts) do
    if opts[:recovery] == true and is_nil(opts[:recovery_authority]) do
      {:error, :invalid_recovery_authority}
    else
      call(log, {:pull, start_after, opts}, opts[:timeout_in_ms] || :infinity)
    end
  end

  @doc """
  Returns the Demux ShardServer for the given shard on this log.

  This is a materializer's only data-plane contact with a log: a one-time
  discovery call (repeated only on failover). All data then flows from the
  ShardServer — chunks for history, buffer for recent transactions.
  """
  @spec get_shard_server(log :: ref(), shard_id :: non_neg_integer()) ::
          {:ok, pid()} | {:error, term()}
  def get_shard_server(log, shard_id), do: call(log, {:get_shard_server, shard_id}, 5_000)

  @doc """
  Encodes an empty transaction at version zero for callers that need one.

  Recovery does not use this as a cursor or progress marker. Empty recovery
  state is represented by persisted WAL metadata, never by a transaction.
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

  Authority is the exact recovery grant `{generation, recovery_id}`. A higher
  generation takes ownership; the same pair is idempotent; an equal-generation
  foreign recovery id is rejected.
  """
  @spec lock_for_recovery(log :: ref(), RecoveryAuthority.input()) ::
          {:ok, pid(),
           recovery_info :: [
             kind: :log,
             last_version: Bedrock.version(),
             available_after: Bedrock.version(),
             oldest_version: Bedrock.version(),
             minimum_durable_version: Bedrock.version() | :unavailable
           ]}
          | {:error, :newer_epoch_exists}
  def lock_for_recovery(storage, authority) do
    case RecoveryAuthority.new(authority) do
      {:ok, canonical} -> call(storage, {:lock_for_recovery, canonical}, :infinity)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Initiates recovery from source logs over `(replay_after, last_inclusive]`.

  The function ensures that the transaction log is consistent with the
  source logs by pulling from them and applying the transactions.

  ## Parameters:

    - `log`: Reference to the target log where recovery should be applied.
    - `authority`: The exact grant that locked both the destination and every
      recovery source.
    - `source_logs`: List of source log references from which transactions are
      recovered, or a single ref for backward compatibility. Empty list or nil
      is sent for initial recovery when there are no source logs.
    - `replay_after`: The exclusive starting cursor. No transaction is created
      at this version.
    - `last_inclusive`: The inclusive endpoint that recovery must observe before
      it can report success.

  ## Multi-Source Recovery (Consistent Hashing)

  When multiple source logs are provided, Shale can try another survivor if a
  source is unavailable. Every log stores the same encoded transaction stream;
  the destination appends each binary unchanged and its Demux performs slicing.

  ## Return Values:

    - `{:ok, pid}`: Recovery was successful, returns the log's PID for subsequent operations.
    - `{:error, :unavailable}`: The log is unavailable, and recovery cannot be
      performed.
  """
  @spec recover_from(
          log :: ref(),
          authority :: RecoveryAuthority.input(),
          source_logs :: [ref()] | ref() | nil,
          replay_after :: Bedrock.version(),
          last_inclusive :: Bedrock.version()
        ) ::
          {:ok, pid()} | {:error, {:failed_to_recover, term()}} | {:error, :unavailable}
  @spec recover_from(ref(), [ref()] | ref() | nil, Bedrock.version(), Bedrock.version()) ::
          {:error, :invalid_recovery_authority}
  def recover_from(_log, _source_logs, _replay_after, _last_inclusive), do: {:error, :invalid_recovery_authority}

  def recover_from(log, authority, source_logs, replay_after, last_inclusive),
    do:
      call(log, {:recover_from, authority, normalize_source_logs(source_logs), replay_after, last_inclusive}, :infinity)

  @doc "Publishes the running checkpoint for the exact authority that completed replay."
  @spec unlock_after_recovery(ref(), RecoveryAuthority.input()) :: :ok | {:error, term()}
  def unlock_after_recovery(log, authority), do: call(log, {:unlock_after_recovery, authority}, :infinity)

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
