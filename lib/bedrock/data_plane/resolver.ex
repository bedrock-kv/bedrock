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

  For detailed conflict detection concepts and architectural integration, see the
  [Resolver documentation](../../../../docs/components/resolver.md).
  """

  alias Bedrock.DataPlane.Transaction

  @type ref :: pid() | atom() | {atom(), node()}

  @spec resolve_transactions(
          ref(),
          epoch :: Bedrock.epoch(),
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [Transaction.encoded()],
          opts :: [timeout: Bedrock.timeout_in_ms()]
        ) ::
          {:ok, aborted :: [transaction_index :: non_neg_integer()]}
          | {:failure, :timeout, ref()}
          | {:failure, :unavailable, ref()}
  def resolve_transactions(ref, epoch, last_version, commit_version, transaction_summaries, opts \\ []) do
    timeout = opts[:timeout] || :infinity

    :telemetry.span(
      [:bedrock, :data_plane, :resolver, :call, :resolve_transactions],
      %{
        resolver_id: ref,
        epoch: epoch,
        last_version: last_version,
        commit_version: commit_version,
        transaction_summaries: transaction_summaries,
        timeout_ms: timeout
      },
      fn ->
        ref
        |> GenServer.call(
          {:resolve_transactions, epoch, {last_version, commit_version}, transaction_summaries},
          timeout
        )
        |> case do
          {:ok, aborted} -> {{:ok, aborted}, %{aborted: aborted}}
          {:error, reason} -> {{:error, reason}, %{}}
        end
      end
    )
  catch
    :exit, {:timeout, _} -> {:failure, :timeout, ref}
    :exit, _reason -> {:failure, :unavailable, ref}
  end
end
