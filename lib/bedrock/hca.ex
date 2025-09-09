defmodule Bedrock.HCA do
  @moduledoc """
  High-Concurrency Allocator (HCA) implementation for Bedrock.

  This module implements the HCA algorithm used by FoundationDB's Directory Layer
  for efficient allocation of unique identifiers in high-concurrency environments.

  Based on the FoundationDB Python bindings and the erlang/erlfdb implementation.
  Uses a sophisticated windowing strategy with:
  1. Randomized candidate selection to reduce contention
  2. Dynamic window sizing based on allocation pressure
  3. Window reuse and cleanup mechanisms
  4. Conflict-resistant allocation patterns

  References:
  - https://ananthakumaran.in/2018/08/05/high-contention-allocator.html
  - https://github.com/foundationdb-beam/erlfdb/blob/main/src/erlfdb_hca.erl
  """

  defstruct [:counters_subspace, :recent_subspace, :repo_module, :random_fn]

  @type t :: %__MODULE__{
          counters_subspace: binary(),
          recent_subspace: binary(),
          repo_module: module(),
          random_fn: (pos_integer() -> pos_integer())
        }

  @doc """
  Create a new HCA instance.

  ## Parameters

    * `repo_module` - The Bedrock.Repo module to use for transactions
    * `subspace` - Binary prefix for this allocator's keys
    * `opts` - Optional configuration

  ## Options

    * `:random_fn` - Custom random function for testing (default: &:rand.uniform/1)

  ## Examples

      iex> hca = Bedrock.HCA.new(MyApp.Repo, "my_allocator")
      iex> hca.counters_subspace
      "my_allocator\\x00"

      iex> hca.recent_subspace
      "my_allocator\\x01"

      # For testing with controlled randomization
      iex> deterministic_random = fn _size -> 1 end
      iex> hca = Bedrock.HCA.new(MyApp.Repo, "test", random_fn: deterministic_random)
  """
  @spec new(module(), binary(), keyword()) :: t()
  def new(repo_module, subspace, opts \\ []) when is_atom(repo_module) and is_binary(subspace) do
    random_fn = Keyword.get(opts, :random_fn, &:rand.uniform/1)

    %__MODULE__{
      repo_module: repo_module,
      counters_subspace: subspace <> <<0>>,
      recent_subspace: subspace <> <<1>>,
      random_fn: random_fn
    }
  end

  @doc """
  Allocate multiple unique IDs from the HCA.

  Returns a list of unique integer IDs. This is implemented by calling
  allocate/2 multiple times.

  ## Examples

      iex> MyApp.Repo.transaction(fn txn ->
      ...>   Bedrock.HCA.allocate_many(hca, txn, 5)
      ...> end)
      {:ok, [0, 1, 2, 3, 4]}
  """
  @spec allocate_many(t(), term(), pos_integer()) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def allocate_many(%__MODULE__{} = hca, txn, count) when is_integer(count) and count > 0 do
    ids =
      for _ <- 1..count do
        case allocate(hca, txn) do
          {:ok, id} -> id
          {:error, reason} -> throw({:error, reason})
        end
      end

    {:ok, ids}
  catch
    {:error, reason} -> {:error, reason}
  end

  @doc """
  Allocate a single unique ID from the HCA.

  Returns a unique integer ID. This operation is highly concurrent and designed
  to minimize write conflicts even under heavy load.

  ## Examples

      iex> MyApp.Repo.transaction(fn txn ->
      ...>   Bedrock.HCA.allocate(hca, txn)
      ...> end)
      {:ok, 42}
  """
  defmodule HCARetryException do
    @moduledoc false
    defexception [:message]

    def new(message \\ "HCA retry needed") do
      %__MODULE__{message: message}
    end
  end

  @spec allocate(t(), term()) :: {:ok, non_neg_integer()} | {:error, term()}
  def allocate(%__MODULE__{} = hca, txn) do
    result = do_allocate(hca, txn)
    {:ok, result}
  rescue
    _error in [HCARetryException] ->
      # Retry the allocation
      allocate(hca, txn)

    error ->
      {:error, error}
  end

  # Private implementation functions

  defp do_allocate(hca, txn) do
    start = current_start(hca, txn)
    {candidate_start, window_size} = get_or_advance_window(hca, txn, start, false)
    search_candidate(hca, txn, candidate_start, window_size)
  end

  defp current_start(hca, txn) do
    # Get the latest counter using KeySelector - equivalent to reverse scan with limit 1!
    counter_range_end = hca.counters_subspace <> <<0xFF>>

    # This KeySelector finds the last key before counter_range_end
    # which is the maximum counter key in our range
    last_counter_selector = Bedrock.KeySelector.last_less_than(counter_range_end)

    case hca.repo_module.select(txn, last_counter_selector) do
      nil ->
        # No counters yet, start at 0
        0

      {resolved_key, _value} ->
        # Verify the resolved key is actually in our counter range
        if String.starts_with?(resolved_key, hca.counters_subspace) do
          decode_counter_key(hca, resolved_key)
        else
          # Key is outside our range, no counters yet
          0
        end
    end
  end

  defp get_or_advance_window(hca, txn, start, window_advanced) do
    # Clear previous window if we advanced
    if window_advanced do
      clear_previous_window(hca, txn, start)
    end

    # Increment counter for this window
    counter_key = encode_counter_key(hca, start)
    _new_count = hca.repo_module.add(txn, counter_key, 1)

    # Get current usage count for this window
    count =
      case hca.repo_module.get(txn, counter_key, snapshot: true) do
        nil -> 0
        <<c::64-little>> -> c
        _other -> 0
      end

    window_size = dynamic_window_size(start)

    # Check if window is getting full (> 50% utilized)
    if count * 2 < window_size do
      # Window still has capacity, use it
      {start, window_size}
    else
      # Window is getting full, advance to next window
      next_start = start + window_size
      get_or_advance_window(hca, txn, next_start, true)
    end
  end

  defp search_candidate(hca, txn, start, window_size) do
    # Generate random candidate within the window
    # Use configurable random function for testing control
    candidate = start + (hca.random_fn.(window_size) - 1)
    candidate_key = encode_recent_key(hca, candidate)

    # Check if we're still in the same counter window (detect concurrent advances)
    current_latest_start = current_start(hca, txn)

    if current_latest_start != start do
      raise HCARetryException.new("Window advanced during allocation")
    end

    # Check if candidate is available and claim it
    case hca.repo_module.get(txn, candidate_key, snapshot: true) do
      nil ->
        # Candidate is available, claim it
        # First set without write conflict to claim the slot
        hca.repo_module.put(txn, candidate_key, "", no_write_conflict: true)

        # Then add write conflict to ensure transaction consistency
        add_write_conflict_key(hca, txn, candidate_key)

        candidate

      _existing_value ->
        # Candidate is taken, retry
        raise HCARetryException.new("Candidate already taken")
    end
  end

  defp clear_previous_window(hca, txn, start) do
    # Clear counter data for this window start
    counter_start = encode_counter_key(hca, start)
    counter_end = next_key(counter_start)
    hca.repo_module.clear_range(txn, counter_start, counter_end, no_write_conflict: true)

    # Clear recent allocation data for this window start
    recent_start = encode_recent_key(hca, start)
    recent_end = next_key(recent_start)
    hca.repo_module.clear_range(txn, recent_start, recent_end, no_write_conflict: true)
  end

  # Dynamic window sizing based on allocation pressure
  defp dynamic_window_size(start) do
    cond do
      start < 255 -> 64
      start < 65_535 -> 1024
      true -> 8192
    end
  end

  # Key encoding functions
  defp encode_counter_key(hca, start) do
    hca.counters_subspace <> <<start::64-big>>
  end

  defp decode_counter_key(hca, counter_key) do
    prefix_size = byte_size(hca.counters_subspace)
    <<_prefix::binary-size(prefix_size), start::64-big>> = counter_key
    start
  end

  defp encode_recent_key(hca, candidate) do
    hca.recent_subspace <> <<candidate::64-big>>
  end

  # Helper function to generate next key for ranges
  defp next_key(key) do
    key <> <<0>>
  end

  # Add write conflict for a specific key - now using Bedrock's native conflict API
  defp add_write_conflict_key(hca, txn, key) do
    # Equivalent to erlfdb:add_write_conflict_key/2
    # This adds write conflict tracking without actually writing to the key
    hca.repo_module.add_write_conflict_range(txn, key, next_key(key))
  end

  @doc """
  Get statistics about the HCA state.

  Returns information about allocated windows, usage patterns, etc.
  Useful for monitoring and debugging.
  """
  @spec stats(t(), term()) :: %{
          latest_window_start: non_neg_integer(),
          total_counters: non_neg_integer(),
          estimated_allocated: non_neg_integer()
        }
  def stats(%__MODULE__{} = hca, txn) do
    latest_start = current_start(hca, txn)

    # Count total counter entries
    counter_start = hca.counters_subspace
    counter_end = hca.counters_subspace <> <<0xFF>>

    counters = txn |> hca.repo_module.range(counter_start, counter_end) |> Enum.to_list()
    total_counters = length(counters)

    # Estimate total allocated IDs by summing counter values
    estimated_allocated =
      Enum.reduce(counters, 0, fn {_key, value}, acc ->
        count =
          case value do
            <<c::64-little>> -> c
            _ -> 0
          end

        acc + count
      end)

    %{
      latest_window_start: latest_start,
      total_counters: total_counters,
      estimated_allocated: estimated_allocated
    }
  end
end
