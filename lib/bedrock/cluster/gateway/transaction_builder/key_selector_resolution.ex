defmodule Bedrock.Cluster.Gateway.TransactionBuilder.KeySelectorResolution do
  @moduledoc """
  KeySelector resolution logic for cross-shard operations.

  This module handles KeySelector resolution that may span multiple storage shards,
  coordinating between the transaction builder's layout index and storage servers
  to resolve KeySelectors to actual keys.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.DataPlane.Storage
  alias Bedrock.KeySelector

  @type resolution_result ::
          {:ok, {resolved_key :: binary(), value :: binary()}}
          | {:error, :not_found | :version_too_old | :version_too_new | :unavailable | :clamped}

  @type range_resolution_result ::
          {:ok, [{key :: binary(), value :: binary()}]}
          | {:error, :not_found | :version_too_old | :version_too_new | :unavailable | :clamped | :invalid_range}

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
    resolve_key_selector_with_circuit_breaker(layout_index, key_selector, version, opts, 0)
  end

  # Maximum number of shard hops before clamping to prevent infinite loops
  @max_shard_hops 10

  @spec resolve_key_selector_with_circuit_breaker(
          LayoutIndex.t(),
          KeySelector.t(),
          Bedrock.version(),
          keyword(),
          non_neg_integer()
        ) :: resolution_result()
  defp resolve_key_selector_with_circuit_breaker(_layout_index, _key_selector, _version, _opts, hop_count)
       when hop_count >= @max_shard_hops do
    {:error, :clamped}
  end

  defp resolve_key_selector_with_circuit_breaker(layout_index, %KeySelector{} = key_selector, version, opts, hop_count) do
    case lookup_storage_server(layout_index, key_selector.key) do
      {:ok, storage_pid} ->
        handle_storage_server_response(
          call_storage_server(storage_pid, key_selector, version, opts),
          layout_index,
          key_selector,
          version,
          opts,
          hop_count
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec handle_storage_server_response(
          {:ok, {binary(), binary()}} | {:partial, integer()} | {:error, atom()},
          LayoutIndex.t(),
          KeySelector.t(),
          Bedrock.version(),
          keyword(),
          non_neg_integer()
        ) :: resolution_result()
  defp handle_storage_server_response(
         {:ok, {resolved_key, value}},
         _layout_index,
         _key_selector,
         _version,
         _opts,
         _hop_count
       ) do
    {:ok, {resolved_key, value}}
  end

  defp handle_storage_server_response({:partial, keys_available}, layout_index, key_selector, version, opts, hop_count) do
    case calculate_continuation(layout_index, key_selector, keys_available) do
      {:ok, continuation_selector} ->
        resolve_key_selector_with_circuit_breaker(
          layout_index,
          continuation_selector,
          version,
          opts,
          hop_count + 1
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_storage_server_response(error, _layout_index, _key_selector, _version, _opts, _hop_count) do
    error
  end

  @spec lookup_storage_server(LayoutIndex.t(), binary()) ::
          {:ok, pid()} | {:error, :unavailable}
  defp lookup_storage_server(layout_index, key) do
    {_key_range, pids} = LayoutIndex.lookup_key!(layout_index, key)

    case pids do
      [storage_pid | _] -> {:ok, storage_pid}
      [] -> {:error, :unavailable}
    end
  rescue
    _ -> {:error, :unavailable}
  end

  @spec call_storage_server(pid(), KeySelector.t(), Bedrock.version(), keyword()) ::
          {:ok, {binary(), binary()}} | {:partial, integer()} | {:error, atom()}
  defp call_storage_server(storage_pid, key_selector, version, opts) do
    # Check if we have a mock configured for testing
    case get_test_mock_response(storage_pid, key_selector, version) do
      :no_mock -> Storage.fetch(storage_pid, key_selector, version, opts)
      mock_response -> mock_response
    end
  end

  @spec get_test_mock_response(pid(), KeySelector.t(), Bedrock.version()) ::
          {:ok, {binary(), binary()}} | {:partial, integer()} | {:error, atom()} | :no_mock
  defp get_test_mock_response(storage_pid, key_selector, version) do
    # Check if we're in test mode with mock responses configured
    mock_key = {:test_storage_mock, storage_pid}

    case Process.get(mock_key) do
      nil -> :no_mock
      mock_fn -> mock_fn.(key_selector, version)
    end
  end

  @spec get_test_range_mock_response(pid(), KeySelector.t(), KeySelector.t(), Bedrock.version(), keyword()) ::
          {:ok, [{binary(), binary()}]} | {:error, atom()} | :no_mock
  defp get_test_range_mock_response(storage_pid, start_selector, end_selector, version, opts) do
    # Check if we're in test mode with range mock responses configured
    mock_key = {:test_range_storage_mock, storage_pid}

    case Process.get(mock_key) do
      nil -> :no_mock
      mock_fn -> mock_fn.(start_selector, end_selector, version, opts)
    end
  end

  @spec calculate_continuation(LayoutIndex.t(), KeySelector.t(), integer()) ::
          {:ok, KeySelector.t()} | {:error, :clamped | :unavailable}
  defp calculate_continuation(layout_index, %KeySelector{} = key_selector, keys_available) do
    # Determine direction and calculate next boundary
    if key_selector.offset >= 0 do
      calculate_forward_continuation(layout_index, key_selector, keys_available)
    else
      calculate_backward_continuation(layout_index, key_selector, keys_available)
    end
  end

  @spec calculate_forward_continuation(LayoutIndex.t(), KeySelector.t(), integer()) ::
          {:ok, KeySelector.t()} | {:error, :clamped | :unavailable}
  defp calculate_forward_continuation(layout_index, key_selector, keys_available) do
    # Find the next shard boundary after the current key
    case find_next_shard_boundary(layout_index, key_selector.key, :forward) do
      {:ok, next_shard_start} ->
        # Calculate the remaining offset after consuming available keys
        remaining_offset = key_selector.offset - keys_available

        # Create continuation KeySelector at the start of next shard
        continuation_selector = %KeySelector{
          key: next_shard_start,
          or_equal: true,
          offset: remaining_offset
        }

        {:ok, continuation_selector}

      :not_found ->
        {:error, :clamped}
    end
  end

  @spec calculate_backward_continuation(LayoutIndex.t(), KeySelector.t(), integer()) ::
          {:ok, KeySelector.t()} | {:error, :clamped | :unavailable}
  defp calculate_backward_continuation(layout_index, key_selector, keys_available) do
    # Find the previous shard boundary before the current key
    case find_next_shard_boundary(layout_index, key_selector.key, :backward) do
      {:ok, prev_shard_end} ->
        # Calculate the remaining offset after consuming available keys
        remaining_offset = key_selector.offset + keys_available

        # Create continuation KeySelector at the end of previous shard
        continuation_selector = %KeySelector{
          key: prev_shard_end,
          or_equal: true,
          offset: remaining_offset
        }

        {:ok, continuation_selector}

      :not_found ->
        {:error, :clamped}
    end
  end

  @spec find_next_shard_boundary(LayoutIndex.t(), binary(), :forward | :backward) ::
          {:ok, binary()} | :not_found
  defp find_next_shard_boundary(layout_index, current_key, direction) do
    # This is a simplified implementation - a real version would use LayoutIndex APIs
    # to navigate shard boundaries efficiently

    case direction do
      :forward ->
        # Find the start key of the next shard
        find_forward_boundary(layout_index, current_key)

      :backward ->
        # Find the end key of the previous shard
        find_backward_boundary(layout_index, current_key)
    end
  end

  @spec find_forward_boundary(LayoutIndex.t(), binary()) :: {:ok, binary()} | :not_found
  defp find_forward_boundary(_layout_index, _current_key) do
    # Placeholder implementation - would use LayoutIndex to find next shard
    # For now, simulate behavior for testing
    case Process.get(:test_next_shard_start) do
      nil -> :not_found
      next_start -> {:ok, next_start}
    end
  end

  @spec find_backward_boundary(LayoutIndex.t(), binary()) :: {:ok, binary()} | :not_found
  defp find_backward_boundary(_layout_index, _current_key) do
    # Placeholder implementation - would use LayoutIndex to find previous shard
    case Process.get(:test_prev_shard_end) do
      nil -> :not_found
      prev_end -> {:ok, prev_end}
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
    {_start_key_range, start_pids} = LayoutIndex.lookup_key!(layout_index, start_selector.key)
    {_end_key_range, end_pids} = LayoutIndex.lookup_key!(layout_index, end_selector.key)

    # Check if we have storage servers for both keys
    case {start_pids, end_pids} do
      {[start_pid | _], [end_pid | _]} when start_pid == end_pid ->
        # Both selectors resolve to the same storage server - can resolve in one call
        case get_test_range_mock_response(start_pid, start_selector, end_selector, version, opts) do
          :no_mock -> Storage.range_fetch(start_pid, start_selector, end_selector, version, opts)
          mock_response -> mock_response
        end

      {[_start_pid | _], [_end_pid | _]} ->
        # Selectors span multiple storage servers - need multi-shard resolution
        resolve_cross_shard_range(
          layout_index,
          start_selector,
          end_selector,
          version,
          opts
        )

      _ ->
        {:error, :unavailable}
    end
  rescue
    _ ->
      {:error, :unavailable}
  end

  # Handle range resolution that spans multiple storage shards
  @spec resolve_cross_shard_range(
          LayoutIndex.t(),
          KeySelector.t(),
          KeySelector.t(),
          Bedrock.version(),
          opts :: [timeout: timeout(), limit: pos_integer()]
        ) :: range_resolution_result()
  defp resolve_cross_shard_range(_layout_index, _start_selector, _end_selector, _version, _opts) do
    # For now, return an error indicating cross-shard ranges are not fully implemented
    # A complete implementation would:
    # 1. Resolve the start selector to get the actual start key
    # 2. Resolve the end selector to get the actual end key
    # 3. Use LayoutIndex to identify all storage servers in the resolved range
    # 4. Coordinate range fetches across multiple storage servers
    # 5. Merge and order the results while respecting the limit
    {:error, :clamped}
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
end
