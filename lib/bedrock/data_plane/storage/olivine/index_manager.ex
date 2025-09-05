defmodule Bedrock.DataPlane.Storage.Olivine.IndexManager do
  @moduledoc """
  Page management core for the Olivine storage driver.

  Implements Phase 2.1 of the Olivine implementation plan:
  - 5-second sliding time window for version retention
  - Version advancement and window expiry
  - Page eviction when versions exit window
  - Version filtering for queries
  - Binary page encoding/decoding with 32-byte header format
  - Page creation and key lookup within pages
  - Simple median split algorithm (256 key threshold)
  - Page ID allocation with max_page_id tracking

  ## Binary Page Format

  Pages are encoded as binary data with the following structure:

  ```
  32-byte header (all big-endian):
  <<PageId:64/big,           # 8 bytes
    NextPageId:64/big,       # 8 bytes
    KeyCount:16/big,         # 2 bytes (supports up to 65535 keys)
    LastKeyOffset:32/big,    # 4 bytes - byte offset to start of last key
    Reserved:80/big,         # 10 bytes
    % Interleaved entries (repeated KeyCount times):
    Version:64/big,          # 8 bytes
    KeyLength:16/big,        # 2 bytes
    Key/binary>>             # KeyLength bytes
  ```

  Keys and versions are stored as interleaved pairs for better cache locality.
  LastKeyOffset points to the start of the last key (after its version and length prefix),
  allowing O(1) access to the last key by reading from that offset to end of binary.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Storage.Olivine.PageAllocator
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.KeySelector

  @type page_id :: Page.id()
  @type page :: Page.t()

  @type loader_fn :: (Bedrock.key(), Bedrock.version() -> {:ok, Bedrock.value()} | {:error, :not_found})

  @type operation :: {:set, Bedrock.version()} | :clear

  @type version_data :: Index.t()
  @type version_update_data :: IndexUpdate.t()

  @opaque t :: %__MODULE__{
            versions: [{Bedrock.version(), version_data()}],
            current_version: Bedrock.version(),
            window_size_in_microseconds: pos_integer(),
            page_allocator: PageAllocator.t()
          }
  defstruct [
    :versions,
    :current_version,
    :window_size_in_microseconds,
    :page_allocator
  ]

  @spec new() :: t()
  def new do
    %__MODULE__{
      versions: [{Version.zero(), Index.new()}],
      current_version: Version.zero(),
      window_size_in_microseconds: 5_000_000,
      page_allocator: PageAllocator.new(0, [])
    }
  end

  @spec recover_from_database(database :: Database.t()) ::
          {:ok, t()} | {:error, :corrupted_page | :broken_chain | :cycle_detected}
  def recover_from_database(database) do
    # Get the durable version from the database
    {:ok, durable_version} = Database.load_durable_version(database)

    # Load the index structure from the database
    case Index.load_from(database) do
      {:ok, initial_index, max_page_id, free_page_ids} ->
        index_manager = %__MODULE__{
          versions: [{durable_version, initial_index}],
          current_version: durable_version,
          window_size_in_microseconds: 5_000_000,
          page_allocator: PageAllocator.new(max_page_id, free_page_ids)
        }

        {:ok, index_manager}

      {:error, reason} when reason in [:corrupted_page, :broken_chain, :cycle_detected, :no_chain] ->
        {:error, reason}
    end
  end

  @spec page_for_key(t(), key :: Bedrock.key(), Bedrock.version()) ::
          {:ok, Page.t()}
          | {:error, :not_found}
          | {:error, :version_too_new}
  def page_for_key(index_manager, _key, version) when index_manager.current_version < version,
    do: {:error, :version_too_new}

  def page_for_key(index_manager, key, version) when is_binary(key) do
    index_manager.versions
    |> find_best_index_for_fetch(version)
    |> case do
      nil ->
        {:error, :version_too_old}

      index ->
        Index.page_for_key(index, key)
    end
  end

  @spec page_for_key(t(), KeySelector.t(), Bedrock.version()) ::
          {:ok, resolved_key :: binary(), Page.t()}
          | {:partial, keys_available :: non_neg_integer()}
          | {:error, :not_found | :version_too_new | :version_too_old}
  def page_for_key(index_manager, %KeySelector{} = _key_selector, version) when index_manager.current_version < version,
    do: {:error, :version_too_new}

  def page_for_key(index_manager, %KeySelector{} = key_selector, version) do
    case find_best_index_for_fetch(index_manager.versions, version) do
      nil ->
        {:error, :version_too_old}

      index ->
        resolve_key_selector_in_index(index, key_selector)
    end
  end

  @spec pages_for_range(t(), start_key :: Bedrock.key(), end_key :: Bedrock.key(), Bedrock.version()) ::
          {:ok, [Page.t()]}
          | {:error, :version_too_new}
          | {:error, :version_too_old}
  def pages_for_range(index_manager, _start_key, _end_key, version) when index_manager.current_version < version,
    do: {:error, :version_too_new}

  def pages_for_range(index_manager, start_key, end_key, version) when is_binary(start_key) and is_binary(end_key) do
    case find_best_index_for_fetch(index_manager.versions, version) do
      nil ->
        {:error, :version_too_old}

      index ->
        Index.pages_for_range(index, start_key, end_key)
    end
  end

  @spec pages_for_range(t(), KeySelector.t(), KeySelector.t(), Bedrock.version()) ::
          {:ok, {resolved_start :: binary(), resolved_end :: binary()}, [Page.t()]}
          | {:error, :version_too_new | :version_too_old | :invalid_range}
  def pages_for_range(index_manager, %KeySelector{} = _start_selector, %KeySelector{} = _end_selector, version)
      when index_manager.current_version < version,
      do: {:error, :version_too_new}

  def pages_for_range(index_manager, %KeySelector{} = start_selector, %KeySelector{} = end_selector, version) do
    case find_best_index_for_fetch(index_manager.versions, version) do
      nil ->
        {:error, :version_too_old}

      index ->
        resolve_range_selectors_in_index(index, start_selector, end_selector)
    end
  end

  @spec apply_transactions(index_manager :: t(), encoded_transactions :: [binary()], database :: Database.t()) :: t()
  def apply_transactions(index_manager, [], _database), do: index_manager

  def apply_transactions(index_manager, transactions, database) when is_list(transactions) do
    Enum.reduce(transactions, index_manager, fn transaction, index_manager ->
      apply_transaction(index_manager, transaction, database)
    end)
  end

  # Transaction Processing Functions (Phase 3.1)

  @doc """
  Applies a single transaction binary to the version manager.
  Creates a new version and applies all mutations in the transaction.
  Uses a two-pass approach: first collect all instructions, then process each page.
  """
  @spec apply_transaction(t(), binary(), Database.t()) :: t()
  def apply_transaction(%{versions: [{_version, current_index} | _]} = index_manager, transaction, database) do
    commit_version = Transaction.commit_version!(transaction)

    {new_index, updated_page_allocator} =
      current_index
      |> IndexUpdate.new(commit_version, index_manager.page_allocator)
      |> IndexUpdate.apply_mutations(Transaction.mutations!(transaction), database)
      |> IndexUpdate.process_pending_operations()
      |> IndexUpdate.store_modified_pages(database)
      |> IndexUpdate.finish()

    %{
      index_manager
      | versions: [{commit_version, new_index} | index_manager.versions],
        current_version: commit_version,
        page_allocator: updated_page_allocator
    }
  end

  # Helper Functions

  @spec last_committed_version(index_manager :: t()) :: Bedrock.version()
  def last_committed_version(index_manager), do: index_manager.current_version

  # These functions are deprecated - durable version is now managed by Database
  @spec last_durable_version(index_manager :: t()) :: nil
  def last_durable_version(_index_manager), do: nil

  @spec oldest_durable_version(index_manager :: t()) :: nil
  def oldest_durable_version(_index_manager), do: nil

  @spec purge_transactions_newer_than(index_manager :: t(), version :: Bedrock.version()) :: :ok
  def purge_transactions_newer_than(_index_manager, _version) do
    :ok
  end

  @spec info(index_manager :: t(), atom()) :: term()
  def info(index_manager, stat) do
    case stat do
      # Key count tracking will be implemented in a future phase.
      # This will require maintaining counters of unique keys across
      # all versions and pages in the version manager.
      :n_keys -> 0
      # Size tracking will be implemented in a future phase.
      # This will require summing the byte size of all pages and
      # values across versions, including lookaside buffer data.
      :size_in_bytes -> 0
      # Utilization tracking will be implemented in a future phase.
      # This will provide metrics on storage efficiency, including
      # page fill ratios and memory usage patterns.
      :utilization -> 0.0
      # Key range tracking will be implemented in a future phase.
      # This will maintain metadata about the range of keys managed
      # by this version manager for partition coordination.
      :key_ranges -> []
      :max_page_id -> index_manager.page_allocator.max_page_id
      :free_page_ids -> index_manager.page_allocator.free_page_ids
      _ -> :undefined
    end
  end

  @doc """
  Determines what needs to happen for window advancement.
  Returns either :no_eviction or {:evict, new_durable_version, evicted_versions, updated_vm}.
  VersionManager handles all the version management logic internally.
  """
  @spec prepare_window_advancement(t()) ::
          :no_eviction
          | {:evict, new_durable_version :: Bedrock.version(), evicted_versions :: [Bedrock.version()],
             updated_vm :: t()}
  def prepare_window_advancement(index_manager) do
    window_start_version = calculate_window_start(index_manager)

    index_manager.versions
    |> split_versions_at_window(window_start_version)
    |> case do
      {_versions_to_keep, []} ->
        :no_eviction

      {versions_to_keep, versions_to_evict} ->
        new_durable_version = elem(List.first(versions_to_evict), 0)
        evicted_versions = Enum.map(versions_to_evict, fn {version, _} -> version end)

        {:evict, new_durable_version, evicted_versions, %{index_manager | versions: versions_to_keep}}
    end
  end

  @doc """
  Calculates the start of the sliding window based on the current version.
  The window is defined relative to the current (highest applied) version.
  For now, we keep all versions - proper windowing can be implemented later
  without breaking the version abstraction.
  """
  @spec calculate_window_start(t()) :: Bedrock.version()
  # Calculate window start based on current version (timestamp) minus window size
  # Versions are microsecond timestamps, so we subtract window_size_in_microseconds
  def calculate_window_start(index_manager) do
    Version.subtract(index_manager.current_version, index_manager.window_size_in_microseconds)
  rescue
    ArgumentError ->
      # Underflow - return zero version
      Version.zero()
  end

  @doc """
  Checks if a version falls within the sliding time window.
  Uses direct binary comparison since versions are lexicographically ordered.
  """
  @spec version_in_window?(Bedrock.version(), Bedrock.version()) :: boolean()
  def version_in_window?(version, window_start_version) do
    version >= window_start_version
  end

  @doc """
  Efficiently splits the versions list at the window boundary.
  Returns {versions_to_keep, versions_to_evict}.
  Since versions list is ordered descending (newest first), we can split at the cutoff point.
  """
  @spec split_versions_at_window([{Bedrock.version(), version_data()}], Bedrock.version()) ::
          {versions_to_keep :: [{Bedrock.version(), version_data()}],
           versions_to_evict :: [{Bedrock.version(), version_data()}]}
  def split_versions_at_window(versions, window_start_version) do
    split_versions_at_window(versions, window_start_version, [])
  end

  # Optimized version splitting using ordered list traversal
  defp split_versions_at_window([], _window_start_version, kept_versions) do
    # No more versions to check, all remaining versions are kept
    {Enum.reverse(kept_versions), []}
  end

  defp split_versions_at_window(
         [{version, _data} = version_entry | rest] = all_versions,
         window_start_version,
         kept_versions
       ) do
    if version_in_window?(version, window_start_version) do
      # This version is still in window, keep it and continue
      split_versions_at_window(rest, window_start_version, [version_entry | kept_versions])
    else
      # This version is outside window, split here
      # All remaining versions (including this one) should be evicted
      {Enum.reverse(kept_versions), all_versions}
    end
  end

  @doc """
  Advances the version manager to a new version with window management.
  - Updates current_version to the new version
  - Evicts expired versions outside the 5-second window
  - Updates durable_version to be the oldest version in the window

  Note: Only transaction application should add entries to the versions list.
  This function only manages version advancement and window eviction.
  """
  @spec advance_version(t(), Bedrock.version()) :: t()
  def advance_version(index_manager, new_version) do
    window_start_version = calculate_window_start(index_manager)

    {versions_to_keep, _versions_to_evict} = split_versions_at_window(index_manager.versions, window_start_version)

    # Note: advance_version only manages in-memory version eviction
    # Persistence should be handled by advance_window_with_persistence before calling this function
    # This separation allows for proper error handling and transaction semantics

    %{index_manager | versions: versions_to_keep, current_version: new_version}
  end

  # Helper Functions for MVCC Value Retrieval (Phase 3.2)

  @doc """
  Finds the best version data for fetch operations using MVCC semantics.
  Returns the latest version that is <= the target version.
  """
  @spec find_best_index_for_fetch(
          [{Bedrock.version(), version_data()}],
          Bedrock.version()
        ) ::
          version_data() | nil
  def find_best_index_for_fetch(versions, target_version) do
    find_first_valid_version(versions, target_version)
  end

  defp find_first_valid_version([], _target_version), do: nil

  defp find_first_valid_version([{version, data} | rest], target_version) do
    if target_version < version do
      find_first_valid_version(rest, target_version)
    else
      data
    end
  end

  # KeySelector Resolution Helper Functions

  @spec resolve_key_selector_in_index(Index.t(), KeySelector.t()) ::
          {:ok, resolved_key :: binary(), Page.t()}
          | {:partial, keys_available :: non_neg_integer()}
          | {:error, :not_found}
  defp resolve_key_selector_in_index(
         index,
         %KeySelector{key: ref_key, or_equal: or_equal, offset: offset} = key_selector
       ) do
    case Index.page_for_key(index, ref_key) do
      {:ok, page} ->
        case resolve_key_selector_in_page(page, ref_key, or_equal, offset) do
          {:ok, resolved_key, page} ->
            {:ok, resolved_key, page}

          {:partial, keys_available} ->
            handle_cross_page_continuation(index, page, key_selector, keys_available)
        end

      {:error, :not_found} ->
        handle_gap_resolution(index, key_selector)
    end
  end

  defp handle_cross_page_continuation(index, page, key_selector, keys_available) do
    # If no keys were available and we have a negative offset, we've gone beyond the start of keyspace
    if keys_available == 0 and key_selector.offset < 0 do
      {:error, :not_found}
    else
      case calculate_cross_page_continuation(index, page, key_selector, keys_available) do
        {:ok, continuation_selector} ->
          resolve_key_selector_in_index(index, continuation_selector)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp handle_gap_resolution(index, key_selector) do
    case resolve_gap_key_selector(index, key_selector) do
      {:ok, continuation_selector} ->
        resolve_key_selector_in_index(index, continuation_selector)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec resolve_range_selectors_in_index(Index.t(), KeySelector.t(), KeySelector.t()) ::
          {:ok, {resolved_start :: binary(), resolved_end :: binary()}, [Page.t()]}
          | {:error, :invalid_range | :not_found}
  defp resolve_range_selectors_in_index(index, start_selector, end_selector) do
    with {:ok, resolved_start, _start_page} <- resolve_key_selector_in_index(index, start_selector),
         {:ok, resolved_end, _end_page} <- resolve_key_selector_in_index(index, end_selector) do
      if resolved_start <= resolved_end do
        {:ok, pages} = Index.pages_for_range(index, resolved_start, resolved_end)
        {:ok, {resolved_start, resolved_end}, pages}
      else
        {:error, :invalid_range}
      end
    end
  end

  @spec resolve_key_selector_in_page(Page.t(), binary(), boolean(), integer()) ::
          {:ok, resolved_key :: binary(), Page.t()}
          | {:partial, keys_available :: non_neg_integer()}
          | {:error, :not_found}
  defp resolve_key_selector_in_page(page, ref_key, or_equal, offset) do
    # Extract header info once for efficient binary operations
    <<_id::unsigned-big-32, _next::unsigned-big-32, key_count::unsigned-big-16, _offset::unsigned-big-32,
      _reserved::unsigned-big-16, entries::binary>> = page

    # Use optimized binary search that stops early when key > ref_key
    case Page.search_entries_with_position(entries, key_count, ref_key) do
      {:found, pos} ->
        target_pos = calculate_target_position_found(pos, or_equal, offset)
        resolve_at_position_optimized(entries, target_pos, key_count, page)

      {:not_found, insertion_pos} ->
        target_pos = calculate_target_position_not_found(insertion_pos, or_equal, offset)
        resolve_at_position_optimized(entries, target_pos, key_count, page)
    end
  end

  # Helper functions for calculating target positions
  defp calculate_target_position_found(pos, or_equal, offset) do
    if or_equal do
      pos + offset
    else
      # For greater_than, we need the next position, then apply offset
      pos + 1 + offset
    end
  end

  defp calculate_target_position_not_found(insertion_pos, or_equal, offset) do
    if offset >= 0 do
      # Forward selector: insertion_pos is correct starting point
      insertion_pos + offset
    else
      # Backward selector: want position before insertion point
      if or_equal do
        # last_less_or_equal: position before insertion point
        insertion_pos - 1 + offset
      else
        # last_less_than: same as less_or_equal when key not found
        insertion_pos - 1 + offset
      end
    end
  end

  defp resolve_at_position_optimized(entries, pos, key_count, page) when pos >= 0 and pos < key_count do
    case Page.decode_entry_at_position(entries, pos, key_count) do
      {:ok, {key, _version}} -> {:ok, key, page}
      :out_of_bounds -> {:partial, key_count}
    end
  end

  defp resolve_at_position_optimized(_entries, pos, _key_count, _page) when pos < 0, do: {:partial, 0}
  defp resolve_at_position_optimized(_entries, _pos, key_count, _page), do: {:partial, key_count}

  # Cross-page continuation helper functions

  @spec calculate_cross_page_continuation(Index.t(), Page.t(), KeySelector.t(), non_neg_integer()) ::
          {:ok, KeySelector.t()} | {:error, :not_found}
  defp calculate_cross_page_continuation(index, current_page, key_selector, keys_available) do
    if key_selector.offset >= 0 do
      calculate_forward_page_continuation(index, current_page, key_selector, keys_available)
    else
      calculate_backward_page_continuation(index, current_page, key_selector, keys_available)
    end
  end

  @spec calculate_forward_page_continuation(Index.t(), Page.t(), KeySelector.t(), non_neg_integer()) ::
          {:ok, KeySelector.t()} | {:error, :not_found}
  defp calculate_forward_page_continuation(index, current_page, key_selector, keys_available) do
    case Page.next_id(current_page) do
      0 ->
        # No next page - we've hit the end of the index
        {:error, :not_found}

      next_page_id ->
        next_page = Index.get_page!(index, next_page_id)

        # Calculate remaining offset after consuming available keys in current page
        remaining_offset = key_selector.offset - keys_available

        case Page.left_key(next_page) do
          nil ->
            # Empty next page, continue with empty key to trigger gap resolution
            continuation_selector = %KeySelector{
              key: "",
              or_equal: true,
              offset: remaining_offset
            }

            {:ok, continuation_selector}

          first_key_of_next_page ->
            # Create continuation KeySelector at the start of next page
            continuation_selector = %KeySelector{
              key: first_key_of_next_page,
              or_equal: true,
              offset: remaining_offset
            }

            {:ok, continuation_selector}
        end
    end
  end

  @spec calculate_backward_page_continuation(Index.t(), Page.t(), KeySelector.t(), non_neg_integer()) ::
          {:ok, KeySelector.t()} | {:error, :not_found}
  defp calculate_backward_page_continuation(index, current_page, key_selector, keys_available) do
    # Find the page whose next_id points to current page (previous page)
    case find_previous_page(index, Page.id(current_page)) do
      {:ok, previous_page} ->
        case Page.right_key(previous_page) do
          nil ->
            # Empty previous page, this shouldn't happen in a well-formed index
            {:error, :not_found}

          last_key_of_prev_page ->
            # Calculate remaining offset after consuming available keys in current page
            # For backward traversal, we add the consumed keys to make the offset more negative
            remaining_offset = key_selector.offset + keys_available

            # Create continuation KeySelector at the end of previous page
            continuation_selector = %KeySelector{
              key: last_key_of_prev_page,
              or_equal: true,
              offset: remaining_offset
            }

            {:ok, continuation_selector}
        end

      {:error, :not_found} ->
        # No previous page - we've hit the beginning of the index
        {:error, :not_found}
    end
  end

  @spec find_previous_page(Index.t(), Page.id()) :: {:ok, Page.t()} | {:error, :not_found}
  defp find_previous_page(%Index{page_map: page_map}, target_page_id) do
    # Search through all pages to find the one whose next_id points to our target
    page_map
    |> Enum.find_value(fn {_page_id, page} ->
      if Page.next_id(page) == target_page_id, do: page
    end)
    |> case do
      nil -> {:error, :not_found}
      page -> {:ok, page}
    end
  end

  @spec resolve_gap_key_selector(Index.t(), KeySelector.t()) ::
          {:ok, KeySelector.t()} | {:error, :not_found}
  defp resolve_gap_key_selector(index, %KeySelector{key: ref_key, offset: offset}) do
    case find_bounding_pages_for_gap(index, ref_key) do
      {:ok, :before_all_pages} -> handle_before_all_pages_gap(index, offset)
      {:ok, :after_all_pages} -> handle_after_all_pages_gap(index, offset)
      {:ok, {:between_pages, _left_page, right_page}} -> handle_between_pages_gap(right_page, offset)
      {:error, reason} -> {:error, reason}
    end
  end

  defp handle_before_all_pages_gap(index, offset) do
    if offset >= 0 do
      case find_first_page(index) do
        {:ok, _first_page, first_key} ->
          {:ok, %KeySelector{key: first_key, or_equal: true, offset: offset}}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, :not_found}
    end
  end

  defp handle_after_all_pages_gap(index, offset) do
    if offset < 0 do
      case find_last_page(index) do
        {:ok, _last_page, last_key} ->
          {:ok, %KeySelector{key: last_key, or_equal: true, offset: offset}}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, :not_found}
    end
  end

  defp handle_between_pages_gap(right_page, offset) do
    case Page.left_key(right_page) do
      nil ->
        {:error, :not_found}

      first_key_of_right when offset >= 0 ->
        {:ok, %KeySelector{key: first_key_of_right, or_equal: true, offset: offset}}

      _first_key_of_right ->
        {:error, :not_found}
    end
  end

  @spec find_bounding_pages_for_gap(Index.t(), binary()) ::
          {:ok, :before_all_pages | :after_all_pages | {:between_pages, Page.t(), Page.t()}}
          | {:error, :not_found}
  defp find_bounding_pages_for_gap(%Index{tree: tree} = index, ref_key) do
    # Use the tree to find the insertion point
    page_id = Tree.page_for_insertion(tree, ref_key)
    page = Index.get_page!(index, page_id)

    case {Page.left_key(page), Page.right_key(page)} do
      {nil, nil} ->
        # Empty page
        {:ok, :before_all_pages}

      {first_key, _last_key} when ref_key < first_key ->
        # Key comes before this page - find previous page
        case find_previous_page(index, page_id) do
          {:ok, prev_page} -> {:ok, {:between_pages, prev_page, page}}
          {:error, :not_found} -> {:ok, :before_all_pages}
        end

      {_first_key, last_key} when ref_key > last_key ->
        # Key comes after this page
        case Page.next_id(page) do
          0 ->
            {:ok, :after_all_pages}

          next_page_id ->
            next_page = Index.get_page!(index, next_page_id)
            {:ok, {:between_pages, page, next_page}}
        end

      _ ->
        # This shouldn't happen since page_for_key already failed
        {:error, :not_found}
    end
  end

  @spec find_first_page(Index.t()) :: {:ok, Page.t(), binary()} | {:error, :not_found}
  defp find_first_page(%Index{page_map: page_map}) when page_map == %{}, do: {:error, :not_found}

  defp find_first_page(%Index{page_map: page_map}) do
    # Start with page 0 and follow the chain to find the first non-empty page
    case Map.get(page_map, 0) do
      nil -> {:error, :not_found}
      page -> find_first_non_empty_page(page_map, page)
    end
  end

  defp find_first_non_empty_page(page_map, page) do
    case Page.left_key(page) do
      nil -> try_next_page(page_map, page)
      first_key -> {:ok, page, first_key}
    end
  end

  defp try_next_page(page_map, page) do
    case Page.next_id(page) do
      0 -> {:error, :not_found}
      next_id -> get_and_check_next_page(page_map, next_id)
    end
  end

  defp get_and_check_next_page(page_map, next_id) do
    case Map.get(page_map, next_id) do
      nil -> {:error, :not_found}
      next_page -> find_first_non_empty_page(page_map, next_page)
    end
  end

  @spec find_last_page(Index.t()) :: {:ok, Page.t(), binary()} | {:error, :not_found}
  defp find_last_page(%Index{page_map: page_map}) when page_map == %{}, do: {:error, :not_found}

  defp find_last_page(%Index{page_map: page_map}) do
    # Find the page with next_id = 0 (last in chain)
    page_map
    |> Enum.find_value(fn {_page_id, page} ->
      if Page.next_id(page) == 0 and Page.right_key(page) != nil do
        {page, Page.right_key(page)}
      end
    end)
    |> case do
      nil -> {:error, :not_found}
      {page, last_key} -> {:ok, page, last_key}
    end
  end
end
