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
  """
  def stream(txn_pid, start_key, end_key, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    timeout = Keyword.get(opts, :timeout, 5000)

    # Filter out stream-specific options, keep only TransactionBuilder options
    txn_opts = Keyword.drop(opts, [:batch_size, :timeout])

    # Initial state tracks the current position in the range
    initial_state = %{
      txn_pid: txn_pid,
      current_key: start_key,
      end_key: end_key,
      txn_opts: txn_opts,
      finished: false,
      items_returned: 0,
      limit: opts[:limit],
      current_batch: [],
      has_more: true
    }

    Stream.resource(
      fn -> initial_state end,
      fn state ->
        if state.finished or limit_reached?(state) do
          {:halt, state}
        else
          emit_next_row(state, batch_size, timeout)
        end
      end,
      fn _state -> :ok end
    )
  end

  defp limit_reached?(%{limit: nil}), do: false
  defp limit_reached?(%{limit: limit, items_returned: returned}), do: returned >= limit

  defp emit_next_row(state, batch_size, timeout) do
    case state.current_batch do
      [] -> fetch_and_emit_first_row(state, batch_size, timeout)
      [row | remaining_rows] -> emit_row_from_buffer(state, row, remaining_rows)
    end
  end

  defp emit_row_from_buffer(state, {key, _} = row, remaining_rows) do
    new_state = %{
      state
      | current_batch: remaining_rows,
        current_key: Bedrock.Key.key_after(key),
        items_returned: state.items_returned + 1,
        finished: remaining_rows == [] and not state.has_more
    }

    {[row], new_state}
  end

  defp fetch_and_emit_first_row(state, batch_size, timeout) do
    effective_batch_size =
      case state.limit do
        nil -> batch_size
        limit -> min(batch_size, limit - state.items_returned)
      end

    case GenServer.call(
           state.txn_pid,
           {:get_range, state.current_key, state.end_key, effective_batch_size, state.txn_opts},
           timeout
         ) do
      {:ok, {[], _}} ->
        {:halt, %{state | finished: true}}

      {:ok, {[first_row | rest], has_more}} ->
        emit_row_from_buffer(%{state | current_batch: rest, has_more: has_more}, first_row, rest)

      {:error, reason} ->
        raise "Range query failed: #{inspect(reason)}"
    end
  end
end
