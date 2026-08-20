defmodule Bedrock.DataPlane.Resolver do
  @moduledoc """
  MVCC conflict detection engine for Bedrock's optimistic concurrency control system.

  The Resolver detects read-write and write-write conflicts by maintaining an interval
  tree that tracks which key ranges were written at which versions. It processes
  transaction batches from Commit Proxies and returns lists of conflicting transaction
  indices to abort.

  Resolvers start in running mode and are immediately ready to process transactions.
  They handle out-of-order transactions through a version-indexed waiting queue that
  ensures consistent conflict detection regardless of network timing variations.

  ## Metadata Distribution

  The Resolver also acts as a distribution point for system metadata mutations
  (keys with \\xFF prefix). Each request includes metadata mutations per
  transaction plus a `metadata_ack` - the calling commit proxy's stable
  identity (its server pid, not the per-batch finalization task pid) and the
  highest metadata window version that proxy has confirmed applying. The
  response includes a differential metadata window covering everything since
  the confirmed version, or `nil` when there is nothing to report. Because
  progress only advances via acks, lost replies are re-sent on the proxy's
  next call and concurrent in-flight windows overlap (out-of-order arrival at
  the proxy is lossless).

  ## Converged verdicts (sharded resolvers)

  With sharded resolvers each resolver only sees its own shard's conflict
  ranges, so a locally-committed transaction can still be aborted globally
  (by another shard's resolver). Metadata therefore travels with its LOCAL
  verdict: every resolver receives every batch's `metadata_per_tx`, records
  each metadata-carrying transaction's mutations together with its own
  verdict, and window entries carry `{mutations, committed?}` pairs. The
  proxy ANDs the verdicts positionally across all resolvers' windows - a
  conflict anywhere vetoes; a resolver holding none of a transaction's
  ranges contributes a trivially-true verdict - so the AND is exactly the
  global verdict, and no resolver ever needs to know it. This is
  FoundationDB's stateMutations relay (each resolver records local
  `committed`; the consuming proxy ANDs across resolvers in
  applyMetadataEffect).
  """

  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Transaction

  @type ref :: pid() | atom() | {atom(), node()}

  @type metadata_mutations :: [Bedrock.Internal.TransactionBuilder.Tx.mutation()]

  @typedoc """
  The calling proxy's stable identity and the highest metadata window
  `to_version` it has confirmed applying (nil if none yet).
  """
  @type metadata_ack :: {proxy_id :: pid(), applied_version :: Bedrock.version() | nil}

  @typedoc """
  A differential window of metadata mutations covering `(from_version,
  to_version]`. `from_version` is nil when coverage starts at the beginning of
  the resolver's history; a `from_version` beyond what the proxy has applied
  signals an unrecoverable coverage gap (the resolver pruned history the proxy
  never confirmed).
  """
  @type metadata_window ::
          {from_version :: Bedrock.version() | nil, to_version :: Bedrock.version(),
           entries :: [MetadataAccumulator.entry()]}
          | nil

  @spec resolve_transactions(
          ref(),
          epoch :: Bedrock.epoch(),
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [Transaction.encoded()],
          metadata_per_tx :: [metadata_mutations()],
          opts :: [
            timeout: Bedrock.timeout_in_ms(),
            metadata_ack: metadata_ack()
          ]
        ) ::
          {:ok, aborted :: [transaction_index :: non_neg_integer()], metadata_window()}
          | {:error, :timeout | :unavailable}
  def resolve_transactions(ref, epoch, last_version, commit_version, transaction_summaries, metadata_per_tx, opts \\ []) do
    timeout = opts[:timeout] || :infinity
    metadata_ack = opts[:metadata_ack] || {self(), nil}

    :telemetry.span(
      [:bedrock, :data_plane, :resolver, :call, :resolve_transactions],
      %{
        resolver_id: ref,
        epoch: epoch,
        last_version: last_version,
        commit_version: commit_version,
        n_transactions: length(transaction_summaries),
        timeout_ms: timeout
      },
      fn ->
        ref
        |> GenServer.call(
          {:resolve_transactions, epoch, {last_version, commit_version}, transaction_summaries, metadata_per_tx,
           metadata_ack},
          timeout
        )
        |> case do
          {:ok, aborted, metadata_window} ->
            {{:ok, aborted, metadata_window}, %{aborted: aborted}}

          {:error, reason} ->
            {{:error, reason}, %{}}
        end
      end
    )
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, _reason -> {:error, :unavailable}
  end
end
