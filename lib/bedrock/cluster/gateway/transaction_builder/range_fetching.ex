defmodule Bedrock.Cluster.Gateway.TransactionBuilder.RangeFetching do
  @moduledoc """
  Range query operations for the Transaction Builder.

  This module handles all range-related fetching operations, including
  version management, storage coordination, and result processing.
  """

  import Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions, only: [next_read_version: 1]

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutUtils
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Internal.Time

  @doc """
  Execute a range batch query against storage servers.

  This function handles version initialization, storage team coordination,
  and result processing for range queries.
  """
  @spec do_range_batch(
          State.t(),
          key_range :: {binary(), binary()},
          batch_size :: pos_integer(),
          opts :: keyword()
        ) ::
          {State.t(),
           {:ok, [{binary(), Bedrock.value()}], :finished | {:continue_from, binary()}}
           | {:error, :not_supported}
           | {:error, :unavailable}
           | {:error, :timeout}}
  def do_range_batch(state, {start_key, end_key}, batch_size, opts \\ []) do
    {:ok, encoded_start_key} = state.key_codec.encode_key(start_key)
    {:ok, encoded_end_key} = state.key_codec.encode_key(end_key)
    encoded_range = {encoded_start_key, encoded_end_key}

    state
    |> ensure_read_version(opts)
    |> storage_servers_for_range(encoded_range)
    |> case do
      {state, []} ->
        {state, {:error, :unavailable}}

      {state, [{_range, storage_pids} | _]} ->
        storage_fetch_fn = Keyword.get(opts, :storage_fetch_fn, &range_fetch_from_storage/5)

        case storage_fetch_fn.(
               storage_pids,
               encoded_range,
               state.read_version,
               batch_size,
               state.fetch_timeout_in_ms
             ) do
          {:ok, %{data: [], has_more: _}} ->
            {state, {:ok, [], :finished}}

          {:ok, %{data: raw_results, has_more: has_more}} when is_list(raw_results) ->
            process_batch_results(state, raw_results, encoded_range, batch_size, has_more)

          {:error, reason} ->
            {state, {:error, reason}}
        end
    end
  end

  defp storage_servers_for_range(state, range),
    do: {state, LayoutUtils.storage_servers_for_range(state.layout_index, range)}

  defp ensure_read_version(%{read_version: nil} = state, opts) do
    next_read_version_fn = Keyword.get(opts, :next_read_version_fn, &next_read_version/1)
    time_fn = Keyword.get(opts, :time_fn, &Time.monotonic_now_in_ms/0)

    case next_read_version_fn.(state) do
      {:ok, read_version, read_version_lease_expiration_in_ms} ->
        read_version_lease_expiration =
          time_fn.() + read_version_lease_expiration_in_ms

        %{state | read_version: read_version, read_version_lease_expiration: read_version_lease_expiration}

      {:error, :unavailable} ->
        raise "No read version available for range query"

      {:error, :lease_expired} ->
        raise "Read version lease expired for range query"
    end
  end

  defp ensure_read_version(state, _opts), do: state

  # Process raw storage results into decoded results and continuation
  defp process_batch_results(state, raw_results, {encoded_start_key, encoded_end_key}, batch_size, has_more) do
    batch_results = merge_storage_with_tx_writes(state.tx, raw_results, {encoded_start_key, encoded_end_key})
    decoded_results = decode_batch_results(batch_results, state)
    updated_tx = record_range_read_in_tx(state, batch_results)
    continuation = determine_continuation(decoded_results, batch_results, batch_size, has_more)

    {%{state | tx: updated_tx}, {:ok, decoded_results, continuation}}
  end

  defp decode_batch_results(batch_results, state) do
    Enum.map(batch_results, fn {encoded_key, encoded_value} ->
      {:ok, decoded_key} = state.key_codec.decode_key(encoded_key)
      {:ok, decoded_value} = state.value_codec.decode_value(encoded_value)
      {decoded_key, decoded_value}
    end)
  end

  defp record_range_read_in_tx(state, batch_results) do
    if Enum.empty?(batch_results) do
      state.tx
    else
      first_key = batch_results |> List.first() |> elem(0)
      last_key = batch_results |> List.last() |> elem(0)
      %{state.tx | range_reads: Tx.add_or_merge(state.tx.range_reads, first_key, last_key)}
    end
  end

  defp determine_continuation(decoded_results, batch_results, batch_size, has_more) do
    cond do
      Enum.empty?(decoded_results) ->
        :finished

      length(batch_results) < batch_size and not has_more ->
        :finished

      true ->
        {last_encoded_key, _} = List.last(batch_results)
        {:continue_from, next_key_after(last_encoded_key)}
    end
  end

  defp merge_storage_with_tx_writes(tx, storage_results, {start_key, end_key}) do
    tx_clear_ranges =
      Enum.filter(tx.mutations, fn
        {:clear_range, s, e} -> LayoutUtils.ranges_overlap?({start_key, end_key}, {s, e})
        _ -> false
      end)

    tx_iterator = :gb_trees.iterator_from(start_key, tx.writes)
    merge_ordered_results(storage_results, tx_iterator, tx_clear_ranges, [])
  end

  defp merge_ordered_results([], _tx_iterator, clear_ranges, acc) do
    acc
    |> filter_cleared_keys(clear_ranges)
    |> Enum.reverse()
  end

  defp merge_ordered_results(storage_list, tx_iterator, clear_ranges, acc) do
    case :gb_trees.next(tx_iterator) do
      {tx_key, tx_value, iterator} ->
        merge_with_tx_write({tx_key, tx_value}, iterator, storage_list, clear_ranges, acc)

      :none ->
        # No more tx writes, add remaining storage results
        remaining_storage = filter_cleared_keys(storage_list, clear_ranges)
        Enum.reverse(acc, remaining_storage)
    end
  end

  defp merge_with_tx_write(
         {tx_key, _tx_value} = tx_kv,
         iterator,
         [{storage_key, _storage_value} = storage_kv | storage_rest] = storage_list,
         clear_ranges,
         acc
       ) do
    cond do
      tx_key < storage_key ->
        merge_ordered_results(storage_list, iterator, clear_ranges, [tx_kv | acc])

      tx_key == storage_key ->
        merge_ordered_results(storage_rest, iterator, clear_ranges, [tx_kv | acc])

      true ->
        merge_ordered_results(storage_rest, iterator, clear_ranges, [storage_kv | acc])
    end
  end

  defp filter_cleared_keys(key_value_pairs, clear_ranges) do
    Enum.reject(key_value_pairs, fn {key, _value} ->
      key_cleared_by_ranges?(key, clear_ranges)
    end)
  end

  defp key_cleared_by_ranges?(key, clear_ranges) do
    Enum.any?(clear_ranges, fn {:clear_range, start_range, end_range} ->
      key >= start_range and key < end_range
    end)
  end

  # Simple increment - add null byte to get next possible key
  defp next_key_after(key) when is_binary(key), do: key <> <<0>>

  @spec range_fetch_from_storage([pid()], {binary(), binary()}, Bedrock.version(), pos_integer(), pos_integer()) ::
          {:ok, %{data: [{binary(), binary()}], has_more: boolean()}}
          | {:error, :unavailable}
          | {:error, :unsupported}
          | {:error, :timeout}
          | {:error, :not_found}
          | {:error, :version_too_old}
          | {:error, :version_too_new}
  defp range_fetch_from_storage(storage_servers, key_range, version, batch_size, timeout),
    do: fetch_from_servers(storage_servers, key_range, version, batch_size, timeout)

  defp fetch_from_servers([], _key_range, _version, _batch_size, _timeout), do: {:error, :unavailable}

  defp fetch_from_servers([storage_server | rest], {min_key, max_key_ex} = key_range, version, batch_size, timeout) do
    case Storage.range_fetch(storage_server, min_key, max_key_ex, version, limit: batch_size, timeout: timeout) do
      {:ok, results} ->
        {:ok, %{data: results, has_more: length(results) == batch_size}}

      {:error, :unavailable} ->
        fetch_from_servers(rest, key_range, version, batch_size, timeout)

      {:error, :unsupported} ->
        fetch_from_servers(rest, key_range, version, batch_size, timeout)

      {:error, _reason} = error ->
        error
    end
  end
end
