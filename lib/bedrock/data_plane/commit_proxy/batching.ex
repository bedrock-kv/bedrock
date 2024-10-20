defmodule Bedrock.DataPlane.CommitProxy.Batching do
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.CommitProxy.Batch

  import Bedrock.DataPlane.Sequencer, only: [next_commit_version: 1]

  import Bedrock.DataPlane.CommitProxy.Batch,
    only: [new_builder: 3, add_transaction: 3, set_finalized_at: 2]

  defp timestamp, do: :erlang.monotonic_time(:millisecond)

  @spec start_batch_if_needed(State.t()) :: State.t()
  def start_batch_if_needed(%{batch: nil} = t) do
    {:ok, last_commit_version, commit_version} =
      next_commit_version(t.transaction_system_layout.sequencer)

    %{t | builder: new_builder(timestamp(), last_commit_version, commit_version)}
  end

  def start_batch_if_needed(t), do: t

  @spec add_transaction_to_batch(State.t(), Bedrock.transaction(), GenServer.from()) :: State.t()
  def add_transaction_to_batch(t, transaction, from),
    do: %{t | batch: t.batch |> add_transaction(transaction, from)}

  @spec apply_finalization_policy(State.t()) ::
          {State.t(), batch_to_finalize :: Batch.t()} | {State.t(), nil}
  def apply_finalization_policy(t) do
    now = timestamp()

    if max_latency?(t.batch, now) or max_transactions?(t.batch, t.max_per_batch) do
      {%{t | batch: nil}, t.batch |> set_finalized_at(now)}
    else
      {t, nil}
    end
  end

  @spec max_latency?(Batch.t(), now :: Bedrock.timestamp_in_ms()) :: boolean()
  defp max_latency?(batch, now),
    do: batch.started_at + batch.max_latency_in_ms < now

  @spec max_transactions?(Batch.t(), max_per_batch :: pos_integer()) :: boolean()
  defp max_transactions?(batch, max_per_batch),
    do: batch.n_transactions >= max_per_batch
end
