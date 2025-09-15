defmodule Bedrock.HighContentionAllocator do
  @moduledoc """
  High-Concurrency Allocator (HCA) implementation for Bedrock.

  This module implements the HighContentionAllocator algorithm used by FoundationDB's Directory Layer
  for efficient allocation of unique identifiers in high-contention environments.

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

  alias Bedrock.Key

  defstruct [:counters_subspace, :recent_subspace, :repo, :random_fn]

  @type t :: %__MODULE__{
          counters_subspace: binary(),
          recent_subspace: binary(),
          repo: module(),
          random_fn: (pos_integer() -> pos_integer())
        }

  @doc """
  Create a new HighContentionAllocator instance.

  ## Parameters

    * `repo` - The Bedrock.Repo module to use for transactions
    * `subspace` - Binary prefix for this allocator's keys
    * `opts` - Optional configuration

  ## Options

    * `:random_fn` - Custom random function for testing (default: &:rand.uniform/1)

  ## Examples

      iex> hca = Bedrock.HighContentionAllocator.new(MyApp.Repo, "my_allocator")
      iex> hca.counters_subspace
      "my_allocator\\x00"

      iex> hca.recent_subspace
      "my_allocator\\x01"

      # For testing with controlled randomization
      iex> deterministic_random = fn _size -> 1 end
      iex> hca = Bedrock.HighContentionAllocator.new(MyApp.Repo, "test", random_fn: deterministic_random)
  """
  @spec new(module(), binary(), keyword()) :: t()
  def new(repo, subspace, opts \\ []) when is_atom(repo) and is_binary(subspace) do
    random_fn = Keyword.get(opts, :random_fn, &:rand.uniform/1)

    %__MODULE__{
      repo: repo,
      counters_subspace: subspace <> <<0>>,
      recent_subspace: subspace <> <<1>>,
      random_fn: random_fn
    }
  end

  @doc """
  Allocate multiple unique IDs from the HighContentionAllocator.

  Returns a list of unique compact binary encoded IDs. This is implemented by calling
  allocate/2 multiple times.

  ## Examples

      iex> MyApp.Repo.transaction(fn txn ->
      ...>   Bedrock.HighContentionAllocator.allocate_many(hca, txn, 5)
      ...> end)
      {:ok, [<<21, 0>>, <<21, 1>>, <<21, 2>>, <<21, 3>>, <<21, 4>>]}
  """
  @spec allocate_many(t(), pos_integer()) :: {:ok, [binary()]} | {:error, term()}
  def allocate_many(%__MODULE__{} = hca, count) when is_integer(count) and count > 0 do
    ids =
      for _ <- 1..count do
        case allocate(hca) do
          {:ok, id} -> id
          {:error, reason} -> throw({:error, reason})
        end
      end

    {:ok, ids}
  catch
    {:error, reason} -> {:error, reason}
  end

  @doc """
  Allocate a single unique ID from the HighContentionAllocator.

  Returns a unique compact binary encoding of the allocated ID. This operation
  is highly concurrent and designed to minimize write conflicts even under heavy load.

  ## Examples

      iex> MyApp.Repo.transaction(fn txn ->
      ...>   Bedrock.HighContentionAllocator.allocate(hca, txn)
      ...> end)
      {:ok, <<21, 42>>}  # Key-encoded binary
  """

  @spec allocate(t()) :: {:ok, binary()} | {:error, term()}
  def allocate(%__MODULE__{repo: repo} = hca) do
    case repo.transaction(&do_allocate(hca, &1)) do
      {:error, _reason} = error -> error
      result -> {:ok, result}
    end
  rescue
    error -> {:error, error}
  end

  @doc """
  Allocate a prefix within an existing transaction.
  """
  @spec allocate(t(), Bedrock.Internal.Repo.transaction()) :: binary()
  def allocate(%__MODULE__{} = hca, txn) do
    do_allocate(hca, txn)
  end

  # Private implementation functions

  defp do_allocate(hca, txn), do: do_allocate_with_retry(hca, txn)

  defp do_allocate_with_retry(hca, txn) do
    start = current_start(hca, txn)
    {candidate_start, window_size} = get_or_advance_window(hca, txn, start, false)
    search_candidate(hca, txn, candidate_start, window_size)
  catch
    {__MODULE__, :retry} ->
      # Retry the allocation within the same transaction
      do_allocate_with_retry(hca, txn)
  end

  defp current_start(%{repo: repo} = hca, txn) do
    # Get the latest counter using KeySelector - equivalent to reverse scan with limit 1!
    counter_range_end = hca.counters_subspace <> <<0xFF>>

    # This KeySelector finds the last key before counter_range_end
    # which is the maximum counter key in our range
    last_counter_selector = Bedrock.KeySelector.last_less_than(counter_range_end)

    case repo.select(txn, last_counter_selector) do
      nil ->
        # No key found by selector, start at 0
        0

      {resolved_key, _value} when is_binary(resolved_key) ->
        # Verify the resolved key is actually in our counter range
        if :binary.match(resolved_key, hca.counters_subspace) == {0, byte_size(hca.counters_subspace)} do
          decode_counter_key(hca, resolved_key)
        else
          # Key is outside our range, no counters yet
          0
        end
    end
  end

  defp get_or_advance_window(%{repo: repo} = hca, txn, start, window_advanced) do
    # Clear previous window if we advanced
    if window_advanced do
      clear_previous_window(hca, txn, start)
    end

    # Increment counter for this window - use little-endian binary encoding
    counter_key = encode_counter_key(hca, start)
    repo.atomic(txn, :add, counter_key, <<1::64-little>>)

    # Get current usage count for this window
    count =
      case repo.get(txn, counter_key, snapshot: true) do
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

  defp search_candidate(%{repo: repo} = hca, txn, start, window_size) do
    # Generate random candidate within the window
    # Use configurable random function for testing control
    candidate = start + (hca.random_fn.(window_size) - 1)
    candidate_key = encode_recent_key(hca, candidate)

    # Check if we're still in the same counter window (detect concurrent advances)
    current_latest_start = current_start(hca, txn)

    if current_latest_start != start do
      throw({__MODULE__, :retry})
    end

    # Check if candidate is available and claim it
    case repo.get(txn, candidate_key, snapshot: true) do
      nil ->
        repo.put(txn, candidate_key, <<>>, no_write_conflict: true)
        add_write_conflict_key(hca, txn, candidate_key)
        Key.pack(candidate)

      _existing_value ->
        # Candidate is taken, retry
        throw({__MODULE__, :retry})
    end
  end

  defp clear_previous_window(%{repo: repo} = hca, txn, start) do
    # Clear counter data for this window start
    counter_key = encode_counter_key(hca, start)
    counter_range = Bedrock.KeyRange.from_prefix(counter_key)
    repo.clear_range(txn, counter_range, no_write_conflict: true)

    # Clear recent allocation data for this window start
    recent_key = encode_recent_key(hca, start)
    recent_range = Bedrock.KeyRange.from_prefix(recent_key)
    repo.clear_range(txn, recent_range, no_write_conflict: true)
  end

  # Dynamic window sizing based on allocation pressure
  defp dynamic_window_size(start) when start < 255, do: 64
  defp dynamic_window_size(start) when start < 65_535, do: 1024
  defp dynamic_window_size(_start), do: 8192

  # Key encoding functions
  defp encode_counter_key(hca, start), do: hca.counters_subspace <> <<start::64-big>>

  defp decode_counter_key(hca, counter_key) do
    prefix_size = byte_size(hca.counters_subspace)
    <<_prefix::binary-size(prefix_size), start::64-big>> = counter_key
    start
  end

  defp encode_recent_key(hca, candidate), do: hca.recent_subspace <> <<candidate::64-big>>

  defp add_write_conflict_key(%{repo: repo}, txn, key), do: repo.add_write_conflict_range(txn, key, Key.key_after(key))

  @doc """
  Get statistics about the HighContentionAllocator state.

  Returns information about allocated windows, usage patterns, etc.
  Useful for monitoring and debugging.
  """
  @spec stats(t()) :: %{
          latest_window_start: non_neg_integer(),
          total_counters: non_neg_integer(),
          estimated_allocated: non_neg_integer()
        }
  def stats(%__MODULE__{repo: repo} = hca) do
    repo.transaction(fn txn ->
      latest_start = current_start(hca, txn)

      # Count total counter entries
      counter_start = hca.counters_subspace
      counter_end = hca.counters_subspace <> <<0xFF>>

      counters = txn |> repo.get_range(counter_start, counter_end) |> Enum.to_list()
      total_counters = length(counters)

      # Estimate total allocated IDs by summing counter values
      estimated_allocated =
        Enum.reduce(counters, 0, fn
          {_key, <<c::64-little>>}, acc -> acc + c
          {_key, _value}, acc -> acc
        end)

      %{
        latest_window_start: latest_start,
        total_counters: total_counters,
        estimated_allocated: estimated_allocated
      }
    end)
  end
end
