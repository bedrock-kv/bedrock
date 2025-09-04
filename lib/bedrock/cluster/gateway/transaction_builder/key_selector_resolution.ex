defmodule Bedrock.Cluster.Gateway.TransactionBuilder.KeySelectorResolution do
  @moduledoc """
  KeySelector resolution logic for cross-shard operations.

  This module handles KeySelector resolution that may span multiple storage shards,
  coordinating between the transaction builder's layout index and storage servers
  to resolve KeySelectors to actual keys.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage
  alias Bedrock.KeySelector

  @type resolution_result ::
          {:ok, Bedrock.key_value()}
          | {:error, :not_found | :version_too_old | :version_too_new | :unavailable}

  @type range_resolution_result ::
          {:ok, [Bedrock.key_value()]}
          | {:error, :not_found | :version_too_old | :version_too_new | :unavailable | :invalid_range}

  @doc """
  Resolve a KeySelector to an actual key and value.

  This function handles the cross-shard resolution process:
  1. Use LayoutIndex to find the appropriate storage server
  2. Call the storage server to resolve the KeySelector
  3. Handle partial resolution by continuing with the next shard if needed
  """
  @spec resolve_key_selector(
          LayoutIndex.t(),
          KeySelector.t(),
          Bedrock.version(),
          opts :: [timeout: timeout()]
        ) :: resolution_result()
  def resolve_key_selector(layout_index, %KeySelector{} = key_selector, version, opts \\ []) do
    with {:ok, storage_pid} <- lookup_storage_server(layout_index, key_selector.key),
         {:ok, {resolved_key, value}} <- call_storage_server(storage_pid, key_selector, version, opts) do
      {:ok, {resolved_key, value}}
    else
      {:partial, keys_available} ->
        case calculate_continuation(layout_index, key_selector, keys_available) do
          {:ok, continuation_selector} ->
            resolve_key_selector(layout_index, continuation_selector, version, opts)

          {:error, :start_of_keyspace} ->
            {:error, :not_found}

          {:error, :end_of_keyspace} ->
            {:error, :not_found}
        end

      error ->
        error
    end
  end

  @spec lookup_storage_server(LayoutIndex.t(), binary()) ::
          {:ok, pid()} | {:error, :unavailable}
  defp lookup_storage_server(layout_index, key) do
    {_key_range, pids} = LayoutIndex.lookup_key!(layout_index, key)

    case pids do
      [] -> {:error, :unavailable}
      pids -> {:ok, Enum.random(pids)}
    end
  rescue
    _ -> {:error, :unavailable}
  end

  @spec call_storage_server(pid(), KeySelector.t(), Bedrock.version(), keyword()) ::
          {:ok, Bedrock.key_value()} | {:partial, integer()} | {:error, atom()}
  defp call_storage_server(storage_pid, key_selector, version, opts) do
    storage_fetch_fn = Keyword.get(opts, :storage_fetch_fn, &Storage.fetch/4)
    storage_fetch_fn.(storage_pid, key_selector, version, opts)
  end

  @spec calculate_continuation(LayoutIndex.t(), KeySelector.t(), integer()) ::
          {:ok, KeySelector.t()} | {:error, :start_of_keyspace | :end_of_keyspace}
  defp calculate_continuation(layout_index, %KeySelector{} = key_selector, keys_available) do
    # Determine direction and calculate next boundary
    if key_selector.offset >= 0 do
      calculate_forward_continuation(layout_index, key_selector, keys_available)
    else
      calculate_backward_continuation(layout_index, key_selector, keys_available)
    end
  end

  @spec calculate_forward_continuation(LayoutIndex.t(), KeySelector.t(), integer()) ::
          {:ok, KeySelector.t()} | {:error, :end_of_keyspace | :unavailable}
  defp calculate_forward_continuation(layout_index, %KeySelector{} = key_selector, keys_available) do
    # Calculate remaining offset after consuming available keys
    remaining_offset = key_selector.offset - keys_available

    # Find the next shard
    case LayoutIndex.get_next_segment(layout_index, key_selector.key) do
      {:ok, {{next_start, _next_end}, _pids}} ->
        # Create continuation selector starting at the next shard
        continuation = %KeySelector{
          key: next_start,
          or_equal: true,
          offset: remaining_offset
        }

        {:ok, continuation}

      :end_of_keyspace ->
        {:error, :end_of_keyspace}
    end
  end

  @spec calculate_backward_continuation(LayoutIndex.t(), KeySelector.t(), integer()) ::
          {:ok, KeySelector.t()} | {:error, :start_of_keyspace | :unavailable}
  defp calculate_backward_continuation(layout_index, %KeySelector{} = key_selector, keys_available) do
    # Calculate remaining offset (negative) after consuming available keys
    remaining_offset = key_selector.offset + keys_available

    # Find the previous shard
    case LayoutIndex.get_previous_segment(layout_index, key_selector.key) do
      {:ok, {{prev_start, _prev_end}, _pids}} ->
        # For backward continuation, we want to start from within the previous shard
        # Use the start of the previous shard range
        continuation = %KeySelector{
          key: prev_start,
          # Include the start key
          or_equal: true,
          offset: remaining_offset
        }

        {:ok, continuation}

      :start_of_keyspace ->
        {:error, :start_of_keyspace}
    end
  end

  @doc """
  Resolve a KeySelector range to actual keys and values.

  This function handles cross-shard range resolution:
  1. Use LayoutIndex to find storage servers for both selectors
  2. Resolve both boundary KeySelectors
  3. Fetch the range using the resolved boundaries
  4. Handle cases where the range spans multiple shards
  """
  @spec resolve_key_selector_range(
          LayoutIndex.t(),
          KeySelector.t(),
          KeySelector.t(),
          Bedrock.version(),
          opts :: [timeout: timeout(), limit: pos_integer()]
        ) :: range_resolution_result()
  def resolve_key_selector_range(
        layout_index,
        %KeySelector{} = start_selector,
        %KeySelector{} = end_selector,
        version,
        opts \\ []
      ) do
    with {:ok, storage_pid} <- lookup_storage_server(layout_index, start_selector.key),
         {:ok, results} <- call_storage_range_server(storage_pid, start_selector, end_selector, version, opts) do
      {:ok, results}
    else
      {:partial, results, continuation_start_selector} ->
        # Range spans shards - continue from where we left off
        case resolve_key_selector_range(layout_index, continuation_start_selector, end_selector, version, opts) do
          {:ok, more_results} ->
            {:ok, results ++ more_results}

          error ->
            error
        end

      error ->
        error
    end
  end

  @spec call_storage_range_server(pid(), KeySelector.t(), KeySelector.t(), Bedrock.version(), keyword()) ::
          {:ok, [Bedrock.key_value()]} | {:partial, [Bedrock.key_value()], KeySelector.t()} | {:error, atom()}
  defp call_storage_range_server(storage_pid, start_selector, end_selector, version, opts) do
    storage_range_fetch_fn = Keyword.get(opts, :storage_range_fetch_fn, &Storage.range_fetch/5)
    storage_range_fetch_fn.(storage_pid, start_selector, end_selector, version, opts)
  end

  @doc """
  Check if a KeySelector might require cross-shard resolution.

  This is a heuristic check that can help optimize resolution by detecting
  cases where resolution will definitely stay within a single shard.
  """
  @spec might_cross_shards?(LayoutIndex.t(), KeySelector.t()) :: boolean()
  def might_cross_shards?(_layout_index, %KeySelector{offset: 0}), do: false
  def might_cross_shards?(_layout_index, %KeySelector{offset: offset}) when abs(offset) <= 10, do: false
  def might_cross_shards?(_layout_index, %KeySelector{offset: _large_offset}), do: true

  @doc """
  Fetch a KeySelector within a transaction builder context.

  This handles the transaction state management in addition to KeySelector resolution.
  """
  @spec do_fetch_key_selector(State.t(), KeySelector.t()) ::
          {State.t(), {:ok, Bedrock.key_value()} | {:error, atom()}}
  def do_fetch_key_selector(t, %KeySelector{} = key_selector) do
    # First check if we have this key in our local writes via the resolved key
    # For now, we simplify and delegate to KeySelectorResolution
    case resolve_key_selector(t.layout_index, key_selector, t.read_version || 0) do
      {:ok, {resolved_key, value}} ->
        # Merge the resolved key and value into transaction state for conflict tracking
        updated_tx = Tx.merge_storage_read(t.tx, resolved_key, value)
        {%{t | tx: updated_tx}, {:ok, {resolved_key, value}}}

      {:error, reason} ->
        {t, {:error, reason}}
    end
  end

  @doc """
  Fetch a KeySelector range within a transaction builder context.

  This handles the transaction state management in addition to KeySelector range resolution.
  """
  @spec do_range_fetch_key_selectors(State.t(), KeySelector.t(), KeySelector.t(), keyword()) ::
          {State.t(), {:ok, [Bedrock.key_value()]} | {:error, atom()}}
  def do_range_fetch_key_selectors(t, start_selector, end_selector, opts) do
    case resolve_key_selector_range(
           t.layout_index,
           start_selector,
           end_selector,
           t.read_version || 0,
           opts
         ) do
      {:ok, results} ->
        # Track the range read using the first and last keys from the results
        updated_t =
          case results do
            [] ->
              # Empty results - no range to track
              t

            [{first_key, _first_value} | _] ->
              {last_key, _last_value} = List.last(results)
              updated_tx = Tx.merge_storage_range_read(t.tx, first_key, last_key, results)
              %{t | tx: updated_tx}
          end

        {updated_t, {:ok, results}}

      {:error, reason} ->
        {t, {:error, reason}}
    end
  end
end
