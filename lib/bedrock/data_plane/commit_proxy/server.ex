defmodule Bedrock.DataPlane.CommitProxy.Server do
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.ClusterController

  import Bedrock.DataPlane.CommitProxy.Batching,
    only: [
      start_batch_if_needed: 1,
      add_transaction_to_batch: 3,
      apply_finalization_policy: 1
    ]

  import Bedrock.DataPlane.CommitProxy.Finalization, only: [finalize_batch: 2]

  use GenServer

  @spec child_spec(
          opts :: [
            controller: pid(),
            transaction_system_layout: TransactionSystemLayout.t(),
            epoch: Bedrock.epoch(),
            max_latency_in_ms: non_neg_integer(),
            max_per_batch: pos_integer()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    controller = opts[:controller] || raise "Missing :controller option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    max_latency_in_ms = opts[:max_latency_in_ms] || 2
    max_per_batch = opts[:max_per_batch] || 10

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {controller, epoch, max_latency_in_ms, max_per_batch}
         ]},
      restart: :temporary
    }
  end

  def init({controller, epoch, max_latency_in_ms, max_per_batch}) do
    %State{
      controller: controller,
      epoch: epoch,
      max_latency_in_ms: max_latency_in_ms,
      max_per_batch: max_per_batch
    }
    |> then(&{:ok, &1})
  end

  # When a transaction is submitted, we check to see if we have a batch already
  # in progress. If we do, we add the transaction to the batch. If we don't, we
  # create a new batch and add the transaction to it. Once added, we check to
  # see if the batch meets the finalization policy. If it does, we finalize the
  # batch. If it doesn't, we wait for a short timeout to see if anything else
  # is submitted before re-considering finalizing the batch.
  def handle_call({:commit, transaction}, from, t) do
    t
    |> ask_for_transaction_system_layout_if_needed()
    |> start_batch_if_needed()
    |> add_transaction_to_batch(transaction, from)
    |> apply_finalization_policy()
    |> case do
      {t, nil} -> t |> noreply(timeout: 0)
      {t, batch} -> t |> noreply(timeout: 0, continue: {:finalize, batch})
    end
  end

  # If we haven't seen any new commit requests come in for a few milliseconds,
  # we go ahead and finalize the batch -- No need to make everyone wait longer
  # than they absolutely need to!
  def handle_info(:timeout, %{batch: nil} = t),
    do: t |> noreply()

  def handle_info(:timeout, %{batch: batch} = t),
    do: %{t | batch: nil} |> noreply(continue: {:finalize, batch})

  def handle_continue({:finalize, batch}, t) do
    :ok = finalize_batch(batch, t.transaction_system_layout)
    t |> noreply()
  end

  def ask_for_transaction_system_layout_if_needed(t) when t.transaction_system_layout == nil,
    do: t

  def ask_for_transaction_system_layout_if_needed(t) do
    t.controller
    |> ClusterController.fetch_transaction_system_layout(50)
    |> case do
      {:ok, transaction_system_layout} ->
        %{t | transaction_system_layout: transaction_system_layout}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp noreply(t, opts \\ [])
  defp noreply(t, continue: continue, timeout: ms), do: {:noreply, t, ms, {:continue, continue}}
  defp noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}
  defp noreply(t, timeout: ms), do: {:noreply, t, ms}
  defp noreply(t, []), do: {:noreply, t}
end
