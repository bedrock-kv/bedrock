defmodule Bedrock.JobQueue.Store do
  @moduledoc """
  Core storage operations for the job queue.

  This module provides the transactional primitives for queue operations,
  following QuiCK paper patterns with Bedrock's ACID guarantees.

  ## Keyspace Layout

      job_queue/
        queues/{queue_id}/
          items/                         # {priority, vesting_time, id} -> Item
          leases/{item_id}               # -> Lease
          stats/pending                  # atomic counter
          stats/processing               # atomic counter

        queue_leases/{queue_id}          # -> QueueLease (two-tier leasing)
        pointers/                        # {vesting_time, queue_id} -> <<>>

  ## Key Encoding

  Item keys use nested tuple encoding `{priority, vesting_time, id}` which:
  - Sorts by priority first (lower = higher priority)
  - Then by vesting_time (earlier = visible first)
  - Then by id for uniqueness

  Pointer keys use `{vesting_time, queue_id}` for efficient scanning of
  queues with visible items.
  """

  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.QueueLease
  alias Bedrock.Keyspace

  @type repo :: module()
  @type root_keyspace :: Keyspace.t()

  @doc """
  Creates keyspaces for a queue.

  Returns a map with keyspaces for items, leases, and stats.
  """
  @spec queue_keyspaces(root_keyspace(), String.t()) :: %{
          items: Keyspace.t(),
          leases: Keyspace.t(),
          stats: Keyspace.t()
        }
  def queue_keyspaces(root, queue_id) do
    queue_ks = Keyspace.partition(root, "queues/#{queue_id}/")

    %{
      items: Keyspace.partition(queue_ks, "items/", key_encoding: TupleEncoding),
      leases: Keyspace.partition(queue_ks, "leases/"),
      stats: Keyspace.partition(queue_ks, "stats/")
    }
  end

  @doc """
  Creates the pointer index keyspace.
  """
  @spec pointer_keyspace(root_keyspace()) :: Keyspace.t()
  def pointer_keyspace(root) do
    Keyspace.partition(root, "pointers/", key_encoding: TupleEncoding)
  end

  @doc """
  Creates the queue leases keyspace for two-tier leasing.
  """
  @spec queue_lease_keyspace(root_keyspace()) :: Keyspace.t()
  def queue_lease_keyspace(root) do
    Keyspace.partition(root, "queue_leases/")
  end

  @doc """
  Obtains an exclusive lease on a queue for dequeuing.

  Per QuiCK paper: Two-tier leasing prevents thundering herd. A consumer
  must first obtain a queue lease before it can dequeue items. Only one
  consumer can hold a queue lease at a time.

  Returns:
  - `{:ok, QueueLease.t()}` - Lease obtained successfully
  - `{:error, :queue_leased}` - Queue already leased by another consumer
  """
  @spec obtain_queue_lease(repo(), root_keyspace(), String.t(), binary(), pos_integer(), keyword()) ::
          {:ok, QueueLease.t()} | {:error, :queue_leased}
  def obtain_queue_lease(repo, root, queue_id, holder, duration_ms, opts \\ []) do
    ks = queue_lease_keyspace(root)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))

    case repo.get(ks, queue_id) do
      nil ->
        # No existing lease - create new one
        lease = QueueLease.new(queue_id, holder, duration_ms: duration_ms, now: now)
        repo.put(ks, queue_id, encode(lease))
        {:ok, lease}

      value ->
        existing = decode(value)

        if existing.expires_at <= now do
          # Existing lease expired - replace it
          lease = QueueLease.new(queue_id, holder, duration_ms: duration_ms, now: now)
          repo.put(ks, queue_id, encode(lease))
          {:ok, lease}
        else
          # Lease still active
          {:error, :queue_leased}
        end
    end
  end

  @doc """
  Releases a queue lease.

  Should be called after finishing dequeue operations to allow other
  consumers to access the queue.
  """
  @spec release_queue_lease(repo(), root_keyspace(), QueueLease.t()) ::
          :ok | {:error, :lease_not_found | :lease_mismatch}
  def release_queue_lease(repo, root, %QueueLease{} = lease) do
    ks = queue_lease_keyspace(root)

    case repo.get(ks, lease.queue_id) do
      nil ->
        {:error, :lease_not_found}

      value ->
        stored = decode(value)

        if stored.id == lease.id do
          repo.clear(ks, lease.queue_id)
          :ok
        else
          {:error, :lease_mismatch}
        end
    end
  end

  @doc """
  Enqueues a job item atomically.

  Within a transaction:
  1. Writes item to queue zone with key {priority, vesting_time, id}
  2. Updates pointer index with atomic min for vesting_time
  3. Increments pending_count via atomic add
  """
  @spec enqueue(repo(), root_keyspace(), Item.t()) :: :ok
  def enqueue(repo, root, %Item{} = item) do
    keyspaces = queue_keyspaces(root, item.queue_id)
    pointers = pointer_keyspace(root)

    # Write item with tuple key
    item_key = Item.key(item)
    repo.put(keyspaces.items, item_key, encode(item))

    # Update pointer index and stats
    update_pointer(repo, pointers, item.vesting_time, item.queue_id)
    update_stats(repo, keyspaces, 1, 0)

    :ok
  end

  @doc """
  Peeks at visible items in a queue, ordered by priority then vesting time.

  Items are visible when:
  - vesting_time <= now
  - lease_id is nil (not currently leased)

  Options:
  - :limit - Maximum items to return (default: 10)
  - :now - Current time in ms (default: System.system_time(:millisecond))
  - :max_scan - Maximum items to scan before giving up (default: limit * 10)
  """
  @spec peek(repo(), root_keyspace(), String.t(), keyword()) :: [Item.t()]
  def peek(repo, root, queue_id, opts \\ []) do
    keyspaces = queue_keyspaces(root, queue_id)
    limit = Keyword.get(opts, :limit, 10)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    max_scan = Keyword.get(opts, :max_scan, limit * 10)

    # Scan items in priority order, collect visible ones
    # Uses Stream to avoid loading all items into memory
    # Stops early once we have enough visible items OR hit max_scan
    keyspaces.items
    |> repo.get_range(limit: max_scan)
    |> Stream.map(fn {_key, value} -> decode(value) end)
    |> Stream.filter(&Item.visible?(&1, now))
    |> Enum.take(limit)
  end

  @doc """
  Atomically dequeues items from a queue.

  Per QuiCK paper: Combines peek + obtain_lease into a single atomic operation.
  This is more efficient than separate calls and avoids race conditions.

  Options:
  - :limit - Maximum items to dequeue (default: 10)
  - :lease_duration - Lease duration in ms (default: 30_000)
  - :now - Current time in ms (default: System.system_time(:millisecond))

  Returns `{:ok, [Lease.t()]}` with leases for successfully dequeued items.
  """
  @spec dequeue(repo(), root_keyspace(), String.t(), binary(), keyword()) :: {:ok, [Lease.t()]}
  def dequeue(repo, root, queue_id, holder, opts \\ []) do
    limit = Keyword.get(opts, :limit, 10)
    lease_duration = Keyword.get(opts, :lease_duration, 30_000)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))

    # Peek for visible items
    items = peek(repo, root, queue_id, limit: limit, now: now)

    # Obtain leases on each item
    leases =
      Enum.reduce(items, [], fn item, acc ->
        case obtain_lease(repo, root, item, holder, lease_duration) do
          {:ok, lease} -> [lease | acc]
          {:error, _} -> acc
        end
      end)

    {:ok, Enum.reverse(leases)}
  end

  @doc """
  Obtains a lease on an item.

  Per QuiCK paper - leasing works by updating the vesting_time to make
  the item invisible to other consumers:

  1. Reads item, verifies it's available
  2. Creates lease record
  3. Updates item's vesting_time to lease expiry (makes it invisible)
  4. Updates pointer index with new min vesting_time
  """
  @spec obtain_lease(repo(), root_keyspace(), Item.t(), binary(), pos_integer()) ::
          {:ok, Lease.t()} | {:error, :already_leased | :not_found}
  def obtain_lease(repo, root, %Item{} = item, holder, duration_ms) do
    keyspaces = queue_keyspaces(root, item.queue_id)
    pointers = pointer_keyspace(root)
    now = System.system_time(:millisecond)

    # Read current item state
    item_key = Item.key(item)

    case repo.get(keyspaces.items, item_key) do
      nil ->
        {:error, :not_found}

      value ->
        current_item = decode(value)

        if current_item.lease_id == nil do
          # Create lease
          lease = Lease.new(current_item, holder, duration_ms: duration_ms, now: now)
          lease_expires_at = now + duration_ms

          # Update item with lease info and new vesting_time
          updated_item = %{
            current_item
            | lease_id: lease.id,
              lease_expires_at: lease_expires_at,
              vesting_time: lease_expires_at
          }

          # Delete old item key (vesting_time changed)
          repo.clear(keyspaces.items, item_key)

          # Write with new key (new vesting_time)
          new_item_key = Item.key(updated_item)
          repo.put(keyspaces.items, new_item_key, encode(updated_item))

          # Write lease record
          repo.put(keyspaces.leases, lease.item_id, encode(lease))

          # Update pointer and stats
          update_pointer(repo, pointers, lease_expires_at, item.queue_id)
          update_stats(repo, keyspaces, -1, 1)

          {:ok, lease}
        else
          {:error, :already_leased}
        end
    end
  end

  @doc """
  Extends a lease's expiration time.

  Per QuiCK paper: Long-running jobs can extend their lease before expiry
  to prevent the item from becoming visible to other consumers.

  1. Validates lease exists and matches
  2. Updates item's vesting_time to new expiry
  3. Updates lease record with new expiry
  4. Updates pointer index
  """
  @spec extend_lease(repo(), root_keyspace(), Lease.t(), pos_integer()) ::
          {:ok, Lease.t()} | {:error, :lease_not_found | :lease_mismatch | :lease_expired}
  def extend_lease(repo, root, %Lease{} = lease, extension_ms) do
    now = System.system_time(:millisecond)

    if lease.expires_at <= now do
      {:error, :lease_expired}
    else
      keyspaces = queue_keyspaces(root, lease.queue_id)

      case verify_lease(repo, keyspaces, lease) do
        {:ok, stored_lease} -> do_extend_lease(repo, root, keyspaces, stored_lease, now + extension_ms)
        error -> error
      end
    end
  end

  defp do_extend_lease(repo, root, keyspaces, stored_lease, new_expires_at) do
    old_item_key = stored_lease.item_key

    case repo.get(keyspaces.items, old_item_key) do
      nil ->
        {:error, :item_not_found}

      item_value ->
        item = decode(item_value)
        updated_item = %{item | vesting_time: new_expires_at, lease_expires_at: new_expires_at}

        # Delete old item key, write with new vesting_time
        repo.clear(keyspaces.items, old_item_key)
        new_item_key = Item.key(updated_item)
        repo.put(keyspaces.items, new_item_key, encode(updated_item))

        # Update lease record
        updated_lease = %{stored_lease | expires_at: new_expires_at, item_key: new_item_key}
        repo.put(keyspaces.leases, stored_lease.item_id, encode(updated_lease))

        # Update pointer index
        update_pointer(repo, pointer_keyspace(root), new_expires_at, stored_lease.queue_id)

        {:ok, updated_lease}
    end
  end

  @doc """
  Completes a leased job, removing it from the queue.

  1. Validates lease exists and matches
  2. Deletes item from queue using stored item_key (O(1) lookup)
  3. Deletes lease record
  4. Decrements processing_count
  """
  @spec complete(repo(), root_keyspace(), Lease.t()) ::
          :ok | {:error, :lease_not_found | :lease_mismatch}
  def complete(repo, root, %Lease{} = lease) do
    keyspaces = queue_keyspaces(root, lease.queue_id)

    case verify_lease(repo, keyspaces, lease) do
      {:ok, stored_lease} ->
        # Use provided item_key or fall back to stored lease's item_key
        item_key = lease.item_key || stored_lease.item_key
        repo.clear(keyspaces.items, item_key)
        repo.clear(keyspaces.leases, lease.item_id)

        update_stats(repo, keyspaces, 0, -1)

        :ok

      error ->
        error
    end
  end

  @doc """
  Requeues a failed job with exponential backoff.

  1. Increments error_count
  2. If exhausted, moves to dead letter queue
  3. Otherwise, sets new vesting_time with backoff
  4. Clears lease
  """
  @spec requeue(repo(), root_keyspace(), Lease.t(), keyword()) ::
          {:ok, :requeued | :dead_lettered} | {:error, term()}
  def requeue(repo, root, %Lease{} = lease, opts) do
    keyspaces = queue_keyspaces(root, lease.queue_id)
    pointers = pointer_keyspace(root)
    now = System.system_time(:millisecond)

    # Get item_key from lease or fetch from stored lease
    with {:ok, item_key} <- resolve_item_key(repo, keyspaces, lease),
         {:ok, item} <- fetch_item(repo, keyspaces, item_key) do
      do_requeue(repo, keyspaces, pointers, lease, item, item_key, opts, now)
    end
  end

  defp resolve_item_key(_repo, _keyspaces, %Lease{item_key: item_key}) when item_key != nil do
    {:ok, item_key}
  end

  defp resolve_item_key(repo, keyspaces, %Lease{} = lease) do
    case verify_lease(repo, keyspaces, lease) do
      {:ok, stored_lease} -> {:ok, stored_lease.item_key}
      error -> error
    end
  end

  defp fetch_item(repo, keyspaces, item_key) do
    case repo.get(keyspaces.items, item_key) do
      nil -> {:error, :item_not_found}
      value -> {:ok, decode(value)}
    end
  end

  defp do_requeue(repo, keyspaces, pointers, lease, item, item_key, opts, now) do
    new_error_count = item.error_count + 1

    if new_error_count >= item.max_retries do
      # Move to dead letter
      move_to_dead_letter(repo, keyspaces, item_key, item)
      repo.clear(keyspaces.leases, lease.item_id)
      {:ok, :dead_lettered}
    else
      # Calculate backoff delay
      delay = calculate_backoff_delay(opts, new_error_count)
      new_vesting_time = now + delay

      # Update item
      updated_item = %{
        item
        | error_count: new_error_count,
          vesting_time: new_vesting_time,
          lease_id: nil,
          lease_expires_at: nil
      }

      # Delete old key, write new
      repo.clear(keyspaces.items, item_key)
      new_item_key = Item.key(updated_item)
      repo.put(keyspaces.items, new_item_key, encode(updated_item))

      # Update pointer, clear lease, update stats
      update_pointer(repo, pointers, new_vesting_time, lease.queue_id)
      repo.clear(keyspaces.leases, lease.item_id)
      update_stats(repo, keyspaces, 1, -1)

      {:ok, :requeued}
    end
  end

  # Calculate backoff delay based on options.
  # If :backoff_fn is provided, use it (standard retry behavior).
  # If :base_delay is provided, use fixed delay with exponential multiplier (snooze behavior).
  defp calculate_backoff_delay(opts, error_count) do
    cond do
      # Explicit base_delay overrides backoff_fn (used by snooze)
      base_delay = Keyword.get(opts, :base_delay) ->
        max_delay = Keyword.get(opts, :max_delay, 60_000)
        (base_delay * :math.pow(2, error_count)) |> trunc() |> min(max_delay)

      # Use backoff function if provided
      backoff_fn = Keyword.get(opts, :backoff_fn) ->
        backoff_fn.(error_count)

      # Default fallback: simple exponential backoff
      true ->
        base = 1000
        max_delay = 60_000
        (base * :math.pow(2, error_count)) |> trunc() |> min(max_delay)
    end
  end

  @doc """
  Gets queue statistics.
  """
  @spec stats(repo(), root_keyspace(), String.t()) :: %{
          pending_count: non_neg_integer(),
          processing_count: non_neg_integer()
        }
  def stats(repo, root, queue_id) do
    keyspaces = queue_keyspaces(root, queue_id)

    pending = decode_counter(repo.get(keyspaces.stats, "pending"))
    processing = decode_counter(repo.get(keyspaces.stats, "processing"))

    %{pending_count: pending, processing_count: processing}
  end

  @doc """
  Gets the minimum vesting_time from items in a queue.

  Per QuiCK Algorithm 2: After dequeuing, read the minimum vesting_time to
  determine when to next scan this queue. Returns nil if queue is empty.

  Options:
  - :limit - Maximum items to scan (default: 1000)
  """
  @spec min_vesting_time(repo(), root_keyspace(), String.t(), keyword()) :: non_neg_integer() | nil
  def min_vesting_time(repo, root, queue_id, opts \\ []) do
    keyspaces = queue_keyspaces(root, queue_id)
    limit = Keyword.get(opts, :limit, 1000)

    # Scan all items and find minimum vesting_time
    # Items are sorted by {priority, vesting_time, id}, so we need to check all
    keyspaces.items
    |> repo.get_range(limit: limit)
    |> Enum.reduce(nil, fn {_key, value}, acc ->
      item = decode(value)

      case acc do
        nil -> item.vesting_time
        min -> min(min, item.vesting_time)
      end
    end)
  end

  @doc """
  Updates a queue's pointer with a new vesting_time.

  Per QuiCK Algorithm 2: After processing, update the pointer to the minimum
  vesting_time of remaining items. This prevents rescanning queues that only
  have future-scheduled items.
  """
  @spec update_queue_pointer(repo(), root_keyspace(), String.t(), non_neg_integer()) :: :ok
  def update_queue_pointer(repo, root, queue_id, vesting_time) do
    pointers = pointer_keyspace(root)
    update_pointer(repo, pointers, vesting_time, queue_id)
    :ok
  end

  @doc """
  Scans the pointer index for queues with visible items.

  Returns queue_ids that have items with vesting_time <= now.
  """
  @spec scan_visible_queues(repo(), root_keyspace(), keyword()) :: [String.t()]
  def scan_visible_queues(repo, root, opts \\ []) do
    pointers = pointer_keyspace(root)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    limit = Keyword.get(opts, :limit, 100)

    # Range from 0 to now+1 (exclusive)
    {start_key, end_key} = pointer_visible_range(now)
    prefix = Keyspace.prefix(pointers)

    {prefix <> start_key, prefix <> end_key}
    |> repo.get_range(limit: limit)
    |> Enum.map(fn {key, _value} ->
      suffix = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))
      {_vesting_time, queue_id} = unpack_pointer_key(suffix)
      queue_id
    end)
    |> Enum.uniq()
  end

  @doc """
  Garbage collects stale pointers.

  Per QuiCK paper: Pointers become stale when their vesting_time has passed
  and the queue has no visible items. This function scans for such pointers
  and deletes them.

  Options:
  - :limit - Maximum pointers to scan (default: 100)
  - :grace_period - Additional time in ms after vesting before GC (default: 60_000)
  - :now - Current time in ms (default: System.system_time(:millisecond))

  Returns the count of deleted pointers.
  """
  @spec gc_stale_pointers(repo(), root_keyspace(), keyword()) :: non_neg_integer()
  def gc_stale_pointers(repo, root, opts \\ []) do
    pointers = pointer_keyspace(root)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    grace_period = Keyword.get(opts, :grace_period, 60_000)
    limit = Keyword.get(opts, :limit, 100)

    # Scan pointers that are past their vesting_time + grace period
    cutoff = now - grace_period
    {start_key, end_key} = pointer_visible_range(cutoff)
    prefix = Keyspace.prefix(pointers)

    stale_pointers =
      {prefix <> start_key, prefix <> end_key}
      |> repo.get_range(limit: limit)
      |> Enum.map(fn {key, value} ->
        suffix = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))
        {vesting_time, queue_id} = unpack_pointer_key(suffix)
        last_active_time = decode_timestamp(value)
        {key, vesting_time, queue_id, last_active_time}
      end)

    # Per QuiCK paper: Delete pointer only if last_active_time + grace_period < now
    # AND queue is actually empty
    Enum.reduce(stale_pointers, 0, fn pointer_info, count ->
      count + maybe_delete_pointer(repo, root, pointer_info, now, grace_period)
    end)
  end

  defp maybe_delete_pointer(repo, root, {key, _vesting_time, queue_id, last_active_time}, now, grace_period) do
    inactive? = now - last_active_time >= grace_period
    empty? = inactive? && queue_empty?(repo, root, queue_id)

    if inactive? && empty? do
      repo.clear(key)
      1
    else
      0
    end
  end

  defp queue_empty?(repo, root, queue_id) do
    keyspaces = queue_keyspaces(root, queue_id)
    repo.get_range(keyspaces.items, limit: 1) == []
  end

  # Private helpers

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)

  # Verifies a lease exists and matches the provided lease ID
  defp verify_lease(repo, keyspaces, %Lease{} = lease) do
    case repo.get(keyspaces.leases, lease.item_id) do
      nil ->
        {:error, :lease_not_found}

      value ->
        stored = decode(value)
        if stored.id == lease.id, do: {:ok, stored}, else: {:error, :lease_mismatch}
    end
  end

  # Updates the pointer index with a new vesting time.
  # Per QuiCK paper: stores last_active_time (when items were last seen) for smarter GC.
  # Uses repo.max to track the most recent activity time.
  defp update_pointer(repo, pointers, vesting_time, queue_id) do
    pointer_key = Keyspace.pack(pointers, {vesting_time, queue_id})
    last_active_time = System.system_time(:millisecond)
    repo.max(pointer_key, encode_timestamp(last_active_time))
  end

  defp encode_timestamp(time), do: <<time::64-little>>

  # Decode timestamp, handling nil or missing values (backward compat)
  defp decode_timestamp(nil), do: 0
  defp decode_timestamp(<<time::64-little>>), do: time
  defp decode_timestamp(_), do: 0

  # Atomically updates pending and processing stats
  defp update_stats(repo, keyspaces, pending_delta, processing_delta) do
    if pending_delta != 0 do
      pending_key = Keyspace.pack(keyspaces.stats, "pending")
      repo.add(pending_key, <<pending_delta::64-signed-little>>)
    end

    if processing_delta != 0 do
      processing_key = Keyspace.pack(keyspaces.stats, "processing")
      repo.add(processing_key, <<processing_delta::64-signed-little>>)
    end
  end

  defp decode_counter(nil), do: 0

  defp decode_counter(<<value::64-signed-little>>), do: max(0, value)

  defp decode_counter(binary) when is_binary(binary) do
    # Handle variable-length little-endian
    size = byte_size(binary) * 8
    <<value::size(size)-signed-little>> = binary
    max(0, value)
  end

  defp move_to_dead_letter(repo, keyspaces, item_key, item) do
    now = System.system_time(:millisecond)
    dead_letter_ks = Keyspace.partition(keyspaces.items, "../dead_letter/")

    # Write to dead letter with failed_at timestamp
    dl_key = "#{now}/#{item.id}"
    repo.put(dead_letter_ks, dl_key, encode(item))

    # Delete from main queue and update stats
    repo.clear(keyspaces.items, item_key)
    update_stats(repo, keyspaces, 0, -1)
  end

  # Pointer key helpers (replacing PointerKey module)

  defp pointer_visible_range(now) do
    start_key = TupleEncoding.pack({0, <<>>})
    end_key = TupleEncoding.pack({now + 1, <<>>})
    {start_key, end_key}
  end

  defp unpack_pointer_key(suffix) do
    TupleEncoding.unpack(suffix)
  end
end
