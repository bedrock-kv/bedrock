defmodule Bedrock.Internal.RangeQuery do
  @moduledoc """
  Client-side streaming interface for range queries.

  This module provides lazy evaluation of range queries, allowing clients
  to control when and how much data to consume, avoiding memory pressure
  and GenServer blocking. The stream automatically handles storage team
  boundaries transparently.

  ## Examples

      # Create a lazy stream directly
      stream = RangeQuery.stream(txn, "start", "end", batch_size: 100)

      # Client controls evaluation
      results = stream
                |> Stream.take_while(&continue_condition/1)
                |> Stream.each(&process_batch/1)
                |> Enum.to_list()
  """

  @doc """
  Create a lazy stream for a range query.

  ## Options

  - `:batch_size` - Number of items to fetch per batch (default: 100)
  - `:timeout` - Timeout per batch request (default: 5000)
  - `:limit` - Maximum total items to return
  - `:mode` - `:individual` to emit individual items, `:batch` to emit batches (default: `:individual`)
  """
  def stream(txn_pid, start_key, end_key, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    timeout = Keyword.get(opts, :timeout, 5000)
    mode = Keyword.get(opts, :mode, :individual)

    # Filter out stream-specific options, keep only TransactionBuilder options
    txn_opts = Keyword.drop(opts, [:batch_size, :timeout, :mode])

    # Initial state tracks the current position in the range
    initial_state = %{
      txn_pid: txn_pid,
      current_key: start_key,
      end_key: end_key,
      txn_opts: txn_opts,
      finished: false,
      items_returned: 0,
      limit: opts[:limit]
    }

    Stream.resource(
      # Start function
      fn -> initial_state end,

      # Next function - fetch next batch from current position
      fn state ->
        if state.finished or limit_reached?(state) do
          {:halt, state}
        else
          fetch_next_batch(state, batch_size, timeout, mode)
        end
      end,

      # After function - no cleanup needed
      fn _state -> :ok end
    )
  end

  # Check if we've hit the limit
  defp limit_reached?(%{limit: nil}), do: false
  defp limit_reached?(%{limit: limit, items_returned: returned}), do: returned >= limit

  # Fetch the next batch starting from current_key
  defp fetch_next_batch(state, batch_size, timeout, mode) do
    # Apply limit to batch size if needed
    effective_batch_size =
      case state.limit do
        nil -> batch_size
        limit -> min(batch_size, limit - state.items_returned)
      end

    case GenServer.call(
           state.txn_pid,
           {:range_batch, state.current_key, state.end_key, effective_batch_size, state.txn_opts},
           timeout
         ) do
      {:ok, {[], _has_more}} ->
        # No more data
        {:halt, %{state | finished: true}}

      {:ok, {batch, false}} when is_list(batch) ->
        # Got final batch - no more data after this
        new_state = %{state | finished: true, items_returned: state.items_returned + length(batch)}
        output = if mode == :individual, do: batch, else: [batch]
        {output, new_state}

      {:ok, {batch, true}} when is_list(batch) ->
        # Got partial batch - more data available, calculate next key
        next_key =
          batch
          |> List.last()
          |> elem(0)
          |> Bedrock.Key.next_key_after()

        new_state = %{state | current_key: next_key, items_returned: state.items_returned + length(batch)}
        output = if mode == :individual, do: batch, else: [batch]
        {output, new_state}

      {:error, reason} ->
        # Error
        raise "Range query failed: #{inspect(reason)}"
    end
  end
end
