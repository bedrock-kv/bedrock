defmodule Bedrock.DataPlane.CommitProxy.Batching do
  @moduledoc false

  import Bedrock.DataPlane.CommitProxy.Batch, only: [new_batch: 4, add_transaction: 4]
  import Bedrock.DataPlane.Sequencer, only: [next_commit_version: 2]

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Transaction

  @spec timestamp() :: Bedrock.timestamp_in_ms()
  defp timestamp, do: :erlang.monotonic_time(:millisecond)

  @spec single_transaction_batch(
          state :: State.t(),
          transaction :: Transaction.encoded(),
          reply_fn :: Batch.reply_fn()
        ) ::
          {:ok, Batch.t()}
          | {:error, :sequencer_unavailable}
  def single_transaction_batch(t, transaction, reply_fn \\ fn _result -> :ok end)

  def single_transaction_batch(%{sequencer: nil}, _transaction, _reply_fn), do: {:error, :sequencer_unavailable}

  def single_transaction_batch(state, transaction, reply_fn) when is_binary(transaction) do
    case next_commit_version(state.sequencer, state.epoch) do
      {:ok, last_commit_version, commit_version, known_committed_version} ->
        {:ok,
         timestamp()
         |> new_batch(last_commit_version, commit_version, known_committed_version)
         |> add_transaction(transaction, reply_fn, :user)}

      {:error, reason} ->
        {:error, {:sequencer_unavailable, reason}}
    end
  end

  @spec start_batch_if_needed(State.t()) :: State.t() | {:error, term()}
  def start_batch_if_needed(%{batch: nil} = t) do
    case next_commit_version(t.sequencer, t.epoch) do
      {:ok, last_commit_version, commit_version, known_committed_version} ->
        %{t | batch: new_batch(timestamp(), last_commit_version, commit_version, known_committed_version)}

      {:error, reason} ->
        {:error, {:sequencer_unavailable, reason}}
    end
  end

  def start_batch_if_needed(t), do: t

  @spec add_transaction_to_batch(State.t(), Transaction.encoded(), Batch.reply_fn(), Batch.commit_mode()) ::
          State.t()
  def add_transaction_to_batch(t, transaction, reply_fn, commit_mode) when is_binary(transaction),
    do: %{t | batch: add_transaction(t.batch, transaction, reply_fn, commit_mode)}

  # How much of the average a new batch replaces. Slow enough that one
  # busy batch does not latch the proxy into waiting, fast enough that
  # real load engages within a few batches.
  @smoothing 0.3

  # Above one, so noise cannot trip it; low enough that light batching
  # still counts as load.
  @batching_threshold 1.5

  # FDB's COMMIT_TRANSACTION_BATCH_INTERVAL_MIN. The sweep found larger
  # holds strictly worse: 8ms cost 12x at idle and lost throughput under
  # load versus 1ms.
  @hold_in_ms 1

  @doc """
  The moving average of batch fill, updated with one finalized batch.
  """
  @spec observe_batch(average :: float(), n_transactions :: non_neg_integer()) :: float()
  def observe_batch(average, n_transactions), do: (1 - @smoothing) * average + @smoothing * n_transactions

  @doc """
  How long to hold an open batch, given recent fill.

  Zero when batches are not filling: an idle proxy must never delay a
  lone transaction, which is what the old unconditional zero timeout got
  right. Otherwise a millisecond, so the finalization round is amortized
  across the transactions that are actually arriving.
  """
  @spec hold_in_ms(average :: float()) :: non_neg_integer()
  def hold_in_ms(average) when average > @batching_threshold, do: @hold_in_ms
  def hold_in_ms(_not_filling), do: 0

  @spec apply_finalization_policy(State.t()) ::
          {State.t(), batch_to_finalize :: Batch.t()} | {State.t(), nil}
  def apply_finalization_policy(t) do
    now = timestamp()

    if max_latency?(t.batch, now, t.max_latency_in_ms) or
         max_transactions?(t.batch, t.max_per_batch) do
      {%{t | batch: nil}, t.batch}
    else
      {t, nil}
    end
  end

  @spec max_latency?(
          Batch.t(),
          now :: Bedrock.timestamp_in_ms(),
          max_latency_in_ms :: pos_integer()
        ) :: boolean()
  defp max_latency?(batch, now, max_latency_in_ms), do: batch.started_at + max_latency_in_ms < now

  @spec max_transactions?(Batch.t(), max_per_batch :: pos_integer()) :: boolean()
  defp max_transactions?(batch, max_per_batch), do: batch.n_transactions >= max_per_batch
end
