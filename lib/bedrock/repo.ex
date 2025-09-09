# credo:disable-for-this-file Credo.Check.Refactor.LongQuoteBlocks
defmodule Bedrock.Repo do
  alias Bedrock.Internal.Repo
  alias Bedrock.Key
  alias Bedrock.KeyRange
  alias Bedrock.KeySelector
  alias Bedrock.Subspace

  @type t :: Repo.transaction()

  # Transaction Control

  @doc """
  Executes a function within a database transaction.

  The function is executed within a transaction context and will be retried
  automatically on conflicts. The transaction is committed when the function
  returns successfully, or rolled back if an exception is raised.

  ## Options

  - `:retry_count` - Maximum number of retries on transaction conflicts (default: varies by implementation)
  - `:timeout_in_ms` - Transaction timeout in milliseconds

  ## Examples

      # Simple transaction
      result = transaction(fn tx ->
        tx
        |> put("key", "value")
        |> get("other_key")
      end)

      # Transaction with options
      transaction(fn tx ->
        put(tx, "counter", "1")
      end, retry_count: 5, timeout_in_ms: 10_000)

  """
  @callback transaction((t() -> result | {:rollback, reason})) :: result | {:rollback, reason}
            when result: t(), reason: any()
  @callback transaction(
              (t() -> result | {:rollback, reason}),
              opts :: [
                retry_count: non_neg_integer(),
                timeout_in_ms: Bedrock.timeout_in_ms()
              ]
            ) :: result | {:rollback, reason}
            when result: t(), reason: any()

  @doc """
  Rolls back the transaction, discarding all changes.

  This cancels the transaction and discards all buffered writes and modifications.
  The transaction cannot be used after rollback.

  ## Examples

      transaction(fn tx ->
        put(tx, "key", "value")

        if should_abort? do
          rollback(tx)
        else
          tx
        end
      end)

  """
  @callback rollback(t()) :: no_return()

  @doc """
  Adds a read conflict key to the transaction.

  This manually adds a key to the transaction's read conflict set without
  actually reading the key. If any other transaction modifies this key,
  this transaction will conflict and retry.

  This is useful for ensuring consistency when you need to conflict on a key
  that you don't actually read but whose modification would invalidate your
  transaction's assumptions.

  ## Examples

      transaction(fn tx ->
        # Add conflict on a counter key without reading it
        tx = add_read_conflict_key(tx, "global_counter")

        # Do other operations that depend on the counter not changing
        put(tx, "dependent_data", compute_based_on_counter())
      end)

  """
  @callback add_read_conflict_key(t(), Key.t()) :: t()

  @doc """
  Adds a write conflict range to the transaction.

  This manually adds a key range to the transaction's write conflict set without
  actually writing to the range. Any other transaction that writes to keys in
  this range will conflict with this transaction.

  This is useful for reserving key ranges or ensuring exclusive access to
  a namespace without actually writing to it.

  ## Examples

      transaction(fn tx ->
        # Reserve the entire user namespace
        tx = add_write_conflict_range(tx, "user:", "user;")

        # Now we can safely assume no other transaction is modifying users
        put(tx, "user:123", user_data)
      end)

  """
  @callback add_write_conflict_range(t(), Key.t(), Key.t()) :: t()
  @callback add_write_conflict_range(t(), Subspace.t() | KeyRange.t()) :: t()

  # Read Operations

  @doc """
  Gets a value for the given key, returning the value or `nil` if not found.

  Keys must be binary and no larger than 16KiB.

  ## Options

  - `:snapshot` - If true, bypasses conflict tracking for this read

  ## Examples

      transaction(fn tx ->
        # Regular read with conflict tracking
        user = get(tx, "user:123")

        # Snapshot read without conflict tracking
        metadata = get(tx, "metadata", snapshot: true)
      end)

  """
  @callback get(t(), Key.t()) :: nil | binary()
  @callback get(t(), Key.t(), opts :: [snapshot: boolean()]) :: nil | binary()

  @doc """
  Selects a key-value pair using a key selector, returning `{key, value}` or `nil` if not found.

  Key selectors provide efficient ways to find keys based on relative positioning
  and prefix matching without requiring exact key knowledge.

  ## Options

  - `:snapshot` - If true, bypasses conflict tracking for this read

  ## Examples

      transaction(fn tx ->
        # Find first key after "user:"
        first_user = select(tx, KeySelector.first_greater_than("user:"))

        # Find last key in user namespace
        last_user = select(tx, KeySelector.last_less_or_equal("user;"))
      end)

  """
  @callback select(t(), KeySelector.t()) :: nil | {Key.t(), binary()}
  @callback select(t(), KeySelector.t(), opts :: [snapshot: boolean()]) :: nil | {Key.t(), binary()}

  # Range Operations

  @doc """
  Lazy range query that returns an enumerable stream of key-value pairs.

  Like FoundationDB's get_range(), this function returns an Enumerable that lazily
  fetches results as needed. For eager collection, pipe to Enum.to_list/1.

  ## Options

  - `:batch_size` - Number of items to fetch per batch (default: 100)
  - `:timeout` - Timeout per batch request (default: 5000)
  - `:limit` - Maximum total items to return
  - `:snapshot` - If true, bypasses conflict tracking (default: false)

  ## Examples

      # Lazy iteration (memory efficient)
      transaction fn tx ->
        tx
        |> range(start_key, end_key, limit: 100)
        |> Enum.take(10)  # Only fetches what's needed
      end

      # Collect all results
      transaction fn tx ->
        results =
          tx
          |> range(start_key, end_key)
          |> Enum.to_list()
      end

      # Snapshot read without conflicts
      transaction fn tx ->
        metadata =
          tx
          |> range("meta/", "meta0", snapshot: true)
          |> Enum.to_list()
      end

  """
  @callback range(
              t(),
              Subspace.t() | KeyRange.t()
            ) :: Enumerable.t(Bedrock.key_value())
  @callback range(
              t(),
              Subspace.t() | KeyRange.t(),
              opts :: [
                batch_size: pos_integer(),
                limit: non_neg_integer(),
                snapshot: boolean(),
                timeout: non_neg_integer()
              ]
            ) :: Enumerable.t(Bedrock.key_value())
  @callback range(t(), Key.t(), Key.t()) :: Enumerable.t(Bedrock.key_value())
  @callback range(
              t(),
              Key.t(),
              Key.t(),
              opts :: [
                batch_size: pos_integer(),
                limit: non_neg_integer(),
                snapshot: boolean(),
                timeout: non_neg_integer()
              ]
            ) :: Enumerable.t(Bedrock.key_value())

  @doc """
  Clears all keys in the specified range.

  Removes all key-value pairs where the key is greater than or equal to start_key
  and less than end_key. This operation is atomic and will be applied when the
  transaction commits.

  ## Options

  - `:no_write_conflict` - If true, disables write conflict detection (default: false)

  ## Examples

      transaction(fn tx ->
        # Clear all user data using key range
        clear_range(tx, "user:", "user;")

        # Clear using subspace
        user_space = Subspace.new("users")
        clear_range(tx, user_space)
      end)

  """
  @callback clear_range(t(), Subspace.t() | KeyRange.t()) :: t()
  @callback clear_range(t(), Subspace.t() | KeyRange.t(), opts :: [no_write_conflict: boolean()]) :: t()
  @callback clear_range(t(), Key.t(), Key.t()) :: t()
  @callback clear_range(t(), Key.t(), Key.t(), opts :: [no_write_conflict: boolean()]) :: t()

  # Write Operations

  @doc """
  Removes a key from the database within the transaction.

  The key is marked for deletion and will be removed when the transaction commits.
  Keys must be binary and no larger than 16KiB.

  ## Options

  - `:no_write_conflict` - If true, disables write conflict detection (default: false)

  ## Examples

      transaction(fn tx ->
        tx
        |> clear("user:123")
        |> clear("index:email:" <> email)
      end)

  """
  @callback clear(t(), Key.t()) :: t()
  @callback clear(t(), Key.t(), opts :: [no_write_conflict: boolean()]) :: t()

  @doc """
  Sets a key to a value within the transaction.

  The write is buffered until the transaction commits. Keys and values must be
  binary data. Keys must be no larger than 16KiB and values no larger than 128KiB.

  ## Options

  - `:no_write_conflict` - If true, disables write conflict detection (default: false)

  ## Examples

      transaction(fn tx ->
        tx
        |> put("user:123", user_data)
        |> put("index:email:" <> email, "user:123")
      end)

  """
  @callback put(t(), Key.t(), value :: binary()) :: t()
  @callback put(t(), Key.t(), value :: binary(), opts :: [no_write_conflict: boolean()]) :: t()

  # Atomic Operations
  #
  # Atomic operations provide "blind write" semantics - they modify values without
  # reading the current state, avoiding all conflicts while maintaining consistency.
  #
  # ## Conflict Behavior
  #
  # - **No Write Conflicts**: Atomic operations do NOT create write conflicts,
  #   enabling maximum concurrency even when multiple transactions modify the same keys
  # - **No Read Conflicts**: Atomic operations do NOT create read conflicts,
  #   allowing high concurrency for counter-like operations
  # - **Local Reads**: If you read a key within the same transaction after an atomic
  #   operation, the computed value is returned without requiring storage access
  # - **Consistency**: Values are computed atomically at commit time using the most
  #   recent storage value, ensuring linearizability
  #
  # ## Examples of Conflict-Free Usage
  #
  #     # Multiple concurrent transactions can increment without any conflicts
  #     transaction fn tx ->
  #       add(tx, "global_counter", 1)  # No conflicts at all
  #     end
  #
  #     # Reading after atomic operations computes locally
  #     transaction fn tx ->
  #       tx = add(tx, "counter", 5)
  #       {:ok, computed_value} = get(tx, "counter")  # Returns computed value
  #       tx
  #     end

  @doc """
  Atomically adds a value to the existing integer stored at the given key.

  If the key does not exist, it is treated as having an initial value of 0. The value must be an integer
  (which will be encoded as little-endian binary). Existing values are interpreted as variable-length
  little-endian integers, with empty binaries treated as 0 for the computation.

  This operation is performed atomically at the storage layer, providing
  strong consistency guarantees even under high contention.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # High-concurrency counter without read conflicts
      transaction fn tx ->
        add(tx, "global_counter", 1)  # Many can run concurrently
      end

      # Computing values locally within transaction
      transaction fn tx ->
        tx = add(tx, "counter", 5)
        {:ok, value} = get(tx, "counter")  # Returns computed value (5 if key was missing)
        tx
      end

  """
  @callback add(t(), Key.t(), value :: integer()) :: t()

  @doc """
  Atomically sets the key to the minimum of the existing value and the provided value.

  If the key does not exist, it is treated as having an initial value of 0, so the result will be
  `min(0, value)`. The value must be an integer (which will be encoded as little-endian binary).
  Existing values are interpreted as variable-length little-endian integers, with empty binaries
  treated as 0 for the computation.

  This operation is performed atomically at the storage layer, providing
  strong consistency guarantees even under high contention.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Setting minimum thresholds without conflicts
      transaction fn tx ->
        min(tx, "max_connections", 100)  # Many can set limits concurrently
      end

      # Computing values locally within transaction
      transaction fn tx ->
        tx = min(tx, "threshold", 50)
        {:ok, value} = get(tx, "threshold")  # Returns min(current_value, 50)
        tx
      end

  """
  @callback min(t(), Key.t(), value :: integer()) :: t()

  @doc """
  Atomically sets the key to the maximum of the existing value and the provided value.

  If the key does not exist, it is treated as having an initial value of 0, so the result will be
  `max(0, value)`. The value must be an integer (which will be encoded as little-endian binary).
  Existing values are interpreted as variable-length little-endian integers, with empty binaries
  treated as 0 for the computation.

  This operation is performed atomically at the storage layer, providing
  strong consistency guarantees even under high contention.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Setting maximum values without conflicts
      transaction fn tx ->
        max(tx, "high_score", 1000)  # Many can update scores concurrently
      end

      # Computing values locally within transaction
      transaction fn tx ->
        tx = max(tx, "peak_usage", 250)
        {:ok, value} = get(tx, "peak_usage")  # Returns max(current_value, 250)
        tx
      end

  """
  @callback max(t(), Key.t(), value :: integer()) :: t()

  @doc """
  Atomically performs bitwise AND on the key with the provided value.

  Both the existing value and operand are treated as little-endian binary values.
  If the key does not exist, returns the operand value.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Clear specific bits without conflicts
      transaction fn tx ->
        bit_and(tx, "permission_flags", <<0b11110000>>)  # Clear lower 4 bits
      end

      # Computing values locally within transaction
      transaction fn tx ->
        tx = bit_and(tx, "flags", <<0b11001100>>)
        {:ok, value} = get(tx, "flags")  # Returns computed AND result
        tx
      end

  """
  @callback bit_and(t(), Key.t(), value :: binary()) :: t()

  @doc """
  Atomically performs bitwise OR on the key with the provided value.

  Both the existing value and operand are treated as little-endian binary values.
  If the key does not exist, it is treated as having an initial value of 0.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Set specific bits without conflicts
      transaction fn tx ->
        bit_or(tx, "permission_flags", <<0b00001111>>)  # Set lower 4 bits
      end

  """
  @callback bit_or(t(), Key.t(), value :: binary()) :: t()

  @doc """
  Atomically performs bitwise XOR on the key with the provided value.

  Both the existing value and operand are treated as little-endian binary values.
  If the key does not exist, it is treated as having an initial value of 0.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Toggle specific bits without conflicts
      transaction fn tx ->
        bit_xor(tx, "toggle_flags", <<0b10101010>>)  # Toggle alternate bits
      end

  """
  @callback bit_xor(t(), Key.t(), value :: binary()) :: t()

  @doc """
  Atomically sets the key to the lexicographically smaller of the existing value and the provided value.

  Uses byte-wise (lexicographic) comparison. If the key does not exist, it is set to the provided value.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Set minimum string value without conflicts
      transaction fn tx ->
        byte_min(tx, "earliest_date", "2024-01-01")
      end

  """
  @callback byte_min(t(), Key.t(), value :: binary()) :: t()

  @doc """
  Atomically sets the key to the lexicographically larger of the existing value and the provided value.

  Uses byte-wise (lexicographic) comparison. If the key does not exist, it is set to the provided value.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Set maximum string value without conflicts
      transaction fn tx ->
        byte_max(tx, "latest_version", "1.2.3")
      end

  """
  @callback byte_max(t(), Key.t(), value :: binary()) :: t()

  @doc """
  Atomically appends the value to the existing key if the result would fit within the size limit.

  The size limit is 131,072 bytes (2^17). If appending would exceed this limit, the key is left unchanged.
  If the key does not exist, it is set to the provided value.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Append to log entries without conflicts
      transaction fn tx ->
        append_if_fits(tx, "log_buffer", "New log entry\\n")
      end

  """
  @callback append_if_fits(t(), Key.t(), value :: binary()) :: t()

  @doc """
  Atomically clears the key if the current value matches the expected value.

  If the key does not exist or the value doesn't match, the key is left unchanged.

  ## Conflict Behavior

  - **No Read Conflicts**: Does NOT create read conflicts - enables high concurrency
  - **No Write Conflicts**: Does NOT create write conflicts - atomic operations never conflict
  - **Blind Write**: Does not read current value from storage during transaction
  - **Local Computation**: Subsequent reads within the same transaction return computed values

  ## Examples

      # Clear a flag only if it has a specific value without conflicts
      transaction fn tx ->
        compare_and_clear(tx, "feature_flag", "enabled")
      end

  """
  @callback compare_and_clear(t(), Key.t(), expected :: binary()) :: t()

  @doc """
  Performs an atomic operation on a key with the provided value.

  This is the low-level atomic operation interface. Most users should use the specific
  atomic operation functions (add, min, max, etc.) instead of calling this directly.

  ## Examples

      # These are equivalent:
      add(tx, "counter", 5)
      atomic(tx, :add, "counter", <<5::64-little>>)

  """
  @callback atomic(t(), operation :: atom(), Key.t(), value :: binary()) :: t()

  defmacro __using__(opts) do
    cluster = Keyword.fetch!(opts, :cluster)

    quote do
      @behaviour Bedrock.Repo

      alias Bedrock.Internal.Repo
      alias Bedrock.Internal.TransactionManager
      alias Bedrock.Subspace

      @cluster unquote(cluster)

      # Transaction Control

      @impl true
      def transaction(fun, opts \\ []), do: TransactionManager.transaction(@cluster, fun, opts)

      @impl true
      defdelegate rollback(t), to: Repo

      @impl true
      defdelegate add_read_conflict_key(t, key), to: Repo

      @impl true
      def add_write_conflict_range(t, subspace_or_range)
      def add_write_conflict_range(t, %Subspace{} = subspace), do: add_write_conflict_range(t, Subspace.range(subspace))
      def add_write_conflict_range(t, {start_key, end_key}), do: Repo.add_write_conflict_range(t, start_key, end_key)

      @impl true
      defdelegate add_write_conflict_range(t, start_key, end_key), to: Repo

      # Read Operations

      @impl true
      def get(t, key) when not is_binary(key) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB")

      def get(t, key), do: Repo.get(t, key, [])

      @impl true
      def get(t, key, opts) when not is_binary(key) or not is_list(opts) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, opts must be list")

      def get(t, key, opts), do: Repo.get(t, key, opts)

      @impl true
      defdelegate select(t, key_selector, opts \\ []), to: Repo

      # Write Operations

      @impl true
      def clear(t, key) when not is_binary(key) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB")

      def clear(t, key), do: Repo.clear(t, key)

      @impl true
      def clear(t, key, _opts) when not is_binary(key) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB")

      def clear(t, key, opts), do: Repo.clear(t, key, opts)

      @impl true
      def put(t, key, value)
          when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384 or byte_size(value) > 131_072,
          do: raise(ArgumentError, "key/value must be binary, key <= 16KiB, value <= 128KiB")

      def put(t, key, value), do: Repo.put(t, key, value)

      @impl true
      def put(t, key, value, _opts)
          when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384 or byte_size(value) > 131_072,
          do: raise(ArgumentError, "key/value must be binary, key <= 16KiB, value <= 128KiB")

      def put(t, key, value, opts), do: Repo.put(t, key, value, opts)

      # Range Operations

      @impl true
      def range(t, subspace_or_range, opts \\ [])
      def range(t, %Subspace{} = subspace, opts), do: range(t, Subspace.range(subspace), opts)
      def range(t, {start_key, end_key}, opts), do: Repo.range(t, start_key, end_key, opts)

      @impl true
      defdelegate range(t, start_key, end_key, opts), to: Repo

      @impl true
      def clear_range(t, subspace_or_range, opts \\ [])
      def clear_range(t, %Subspace{} = subspace, opts), do: clear_range(t, Subspace.range(subspace), opts)
      def clear_range(t, {start_key, end_key}, opts), do: Repo.clear_range(t, start_key, end_key, opts)

      @impl true
      defdelegate clear_range(t, start_key, end_key, opts), to: Repo

      # Atomic Operations

      @impl true
      def add(t, key, value), do: Repo.atomic(t, :add, key, <<value::64-little>>)

      @impl true
      def min(t, key, value), do: Repo.atomic(t, :min, key, <<value::64-little>>)

      @impl true
      def max(t, key, value), do: Repo.atomic(t, :max, key, <<value::64-little>>)

      @impl true
      def bit_and(t, key, value), do: Repo.atomic(t, :bit_and, key, value)

      @impl true
      def bit_or(t, key, value), do: Repo.atomic(t, :bit_or, key, value)

      @impl true
      def bit_xor(t, key, value), do: Repo.atomic(t, :bit_xor, key, value)

      @impl true
      def byte_min(t, key, value), do: Repo.atomic(t, :byte_min, key, value)

      @impl true
      def byte_max(t, key, value), do: Repo.atomic(t, :byte_max, key, value)

      @impl true
      def append_if_fits(t, key, value), do: Repo.atomic(t, :append_if_fits, key, value)

      @impl true
      def compare_and_clear(t, key, expected)
          when not is_binary(key) or not is_binary(expected) or byte_size(key) > 16_384,
          do: raise(ArgumentError, "key must be binary and no larger than 16KiB, expected must be binary")

      def compare_and_clear(t, key, expected), do: Repo.atomic(t, :compare_and_clear, key, expected)

      @impl true
      def atomic(t, op, key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      defdelegate atomic(t, op, key, value), to: Repo
    end
  end
end
