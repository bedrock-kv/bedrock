# credo:disable-for-this-file Credo.Check.Refactor.LongQuoteBlocks
defmodule Bedrock.Repo do
  alias Bedrock.Internal.Repo
  alias Bedrock.Key
  alias Bedrock.KeySelector
  alias Bedrock.Keyspace

  # Transaction Control

  @doc """
  Executes a function within a database transaction.

  The function is executed within a transaction context and will be retried
  automatically on conflicts. The transaction is committed when the function
  returns successfully, or rolled back if an exception is raised.

  ## Options

  - `:retry_limit` - Maximum number of retries on transaction conflicts (default: nil for unlimited)
  - `:timeout_in_ms` - Transaction timeout in milliseconds

  ## Examples

      # Simple transaction
      {:ok, result} = transact(fn ->
        put("key", "value")
        result = get("other_key")
        {:ok, result}
      end)

      # Transaction with retry limit
      {:ok, :ok} = transact(fn ->
        put("counter", "1")
        {:ok, :ok}
      end, retry_limit: 5)

  """
  @callback transact(
              (-> :ok | {:ok, result} | {:error, reason})
              | (module() -> :ok | {:ok, result} | {:error, reason})
            ) ::
              :ok | {:ok, result} | {:error, reason}
            when result: term(), reason: term()
  @callback transact(
              (-> :ok | {:ok, result} | {:error, reason})
              | (module() -> :ok | {:ok, result} | {:error, reason}),
              opts :: [
                retry_limit: non_neg_integer() | nil,
                timeout_in_ms: Bedrock.timeout_in_ms()
              ]
            ) :: :ok | {:ok, result} | {:error, reason}
            when result: term(), reason: any()

  @doc """
  Rolls back the transaction, discarding all changes.

  This cancels the transaction and discards all buffered writes and modifications. The transaction cannot be used after
  rollback.

  ## Examples

      {:error, :some_reason} = transact(fn ->
        put("key", "value")

        if should_abort? do
          rollback(:some_reason)
        else
          {:ok, :ok}
        end
      end)

  """
  @callback rollback(reason :: term()) :: no_return()

  @doc """
  Adds a read conflict key to the transaction.

  This manually adds a key to the transaction's read conflict set without
  actually reading the key. If any other transaction modifies this key,
  this transaction will conflict and retry.

  This is useful for ensuring consistency when you need to conflict on a key
  that you don't actually read but whose modification would invalidate your
  transaction's assumptions.

  ## Examples

      transact(fn ->
        # Add conflict on a counter key without reading it
        add_read_conflict_key("global_counter")

        # Do other operations that depend on the counter not changing
        put("dependent_data", compute_based_on_counter())

        {:ok, :ok}
      end)

  """
  @callback add_read_conflict_key(Key.t()) :: :ok

  @doc """
  Adds a write conflict range to the transaction.

  This manually adds a key range to the transaction's write conflict set without
  actually writing to the range. Any other transaction that writes to keys in
  this range will conflict with this transaction.

  This is useful for reserving key ranges or ensuring exclusive access to
  a namespace without actually writing to it.

  ## Examples

      transact(fn ->
        # Reserve the entire user namespace
        add_write_conflict_range("user:", "user;")

        # Now we can safely assume no other transaction is modifying users
        put("user:123", user_data)

        {:ok, :ok}
      end)

  """
  @callback add_write_conflict_range(Bedrock.ToKeyRange.t()) :: :ok

  # Read Operations

  @doc """
  Gets a value for the given key, returning the value or `nil` if not found.

  Keys must be binary and no larger than 16KiB.

  ## Options

  - `:snapshot` - If true, bypasses conflict tracking for this read

  ## Examples

      transact(fn ->
        # Regular read with conflict tracking
        user = get("user:123")

        # Snapshot read without conflict tracking
        metadata = get("metadata", snapshot: true)

        {:ok, :ok}
      end)

  """
  @callback get(key :: binary()) :: nil | binary()
  @callback get(key :: binary(), opts :: [snapshot: boolean()]) :: nil | binary()

  @callback get(Keyspace.t(), key :: binary()) :: nil | binary()
  @callback get(Keyspace.t(), key :: binary(), opts :: [snapshot: boolean()]) :: nil | binary()

  @doc """
  Selects a key-value pair using a key selector, returning `{key, value}` or `nil` if not found.

  Key selectors provide efficient ways to find keys based on relative positioning
  and prefix matching without requiring exact key knowledge.

  ## Options

  - `:snapshot` - If true, bypasses conflict tracking for this read

  ## Examples

      transact(fn ->
        # Find first key after "user:"
        first_user = select(KeySelector.first_greater_than("user:"))

        # Find last key in user namespace
        last_user = select(KeySelector.last_less_or_equal("user;"))

        {:ok, :ok}
      end)

  """
  @callback select(KeySelector.t()) :: nil | {Key.t(), binary()}
  @callback select(KeySelector.t(), opts :: [snapshot: boolean()]) :: nil | {Key.t(), binary()}

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
      transact(fn ->
        result =
          get_range(start_key, end_key, limit: 100)
          |> Enum.take(10)  # Only fetches what's needed

        {:ok, result}
      end)

      # Collect all results
      transact(fn ->
        results =
          get_range(start_key, end_key)
          |> Enum.to_list()

        {:ok, results}
      end)

      # Snapshot read without conflicts
      transact(fn ->
        metadata =
          get_range("meta/", "meta0", snapshot: true)
          |> Enum.to_list()

        {:ok, metadata}
      end)

  """
  @callback get_range(Bedrock.ToKeyRange.t()) :: Enumerable.t(Bedrock.key_value())
  @callback get_range(
              Bedrock.ToKeyRange.t(),
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

      transact(fn ->
        # Clear all user data using key range
        clear_range("user:", "user;")

        # Clear using subspace
        user_space = Keyspace.new("users")
        clear_range(user_space)

        {:ok, :ok}
      end)

  """
  @callback clear_range(Bedrock.ToKeyRange.t()) :: :ok
  @callback clear_range(Bedrock.ToKeyRange.t(), opts :: [no_write_conflict: boolean()]) :: :ok

  # Write Operations

  @doc """
  Removes a key from the database within the transaction.

  The key is marked for deletion and will be removed when the transaction commits.
  Keys must be binary and no larger than 16KiB.

  ## Options

  - `:no_write_conflict` - If true, disables write conflict detection (default: false)

  ## Examples

      transact(fn ->
        clear("user:123")
        clear("index:email:" <> email)
      end)

  """
  @callback clear(Key.t()) :: :ok
  @callback clear(Key.t(), opts :: [no_write_conflict: boolean()]) :: :ok

  @callback clear(Keyspace.t(), key :: binary()) :: :ok
  @callback clear(Keyspace.t(), key :: binary(), opts :: [no_write_conflict: boolean()]) :: :ok

  @doc """
  Sets a key to a value within the transaction.

  The write is buffered until the transaction commits. Keys and values must be
  binary data. Keys must be no larger than 16KiB and values no larger than 128KiB.

  ## Options

  - `:no_write_conflict` - If true, disables write conflict detection (default: false)

  ## Examples

      transact(fn ->
        put("user:123", user_data)
        put("index:email:" <> email, "user:123")
        {:ok, :ok}
      end)

  """
  @callback put(Key.t(), value :: binary()) :: :ok
  @callback put(Key.t(), value :: binary(), opts :: [no_write_conflict: boolean()]) :: :ok

  @callback put(Keyspace.t(), key :: binary(), value :: term()) :: :ok
  @callback put(Keyspace.t(), key :: binary(), value :: term(), opts :: [no_write_conflict: boolean()]) :: :ok

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
  #     transaction fn ->
  #       add("global_counter", 1)  # No conflicts at all
  #       {:ok, :ok}
  #     end
  #
  #     # Reading after atomic operations computes locally
  #     transaction fn ->
  #       add("counter", 5)
  #       {:ok, computed_value} = get("counter")  # Returns computed value
  #       {:ok, computed_value}
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
      transaction fn ->
        add("global_counter", 1)  # Many can run concurrently
        {:ok, :ok}
      end

      # Computing values locally within transaction
      transaction fn ->
        add("counter", 5)
        {:ok, value} = get("counter")  # Returns computed value (5 if key was missing)
        {:ok, value}
      end

  """
  @callback add(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        min("max_connections", 100)  # Many can set limits concurrently
        {:ok, :ok}
      end

      # Computing values locally within transaction
      transaction fn ->
        min("threshold", 50)
        {:ok, value} = get("threshold")  # Returns min(current_value, 50)
        {:ok, value}
      end

  """
  @callback min(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        max("high_score", 1000)  # Many can update scores concurrently
        {:ok, :ok}
      end

      # Computing values locally within transaction
      transaction fn ->
        max("peak_usage", 250)
        {:ok, value} = get("peak_usage")  # Returns max(current_value, 250)
        {:ok, value}
      end

  """
  @callback max(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        bit_and("permission_flags", <<0b11110000>>)  # Clear lower 4 bits
        {:ok, :ok}
      end

      # Computing values locally within transaction
      transaction fn ->
        bit_and("flags", <<0b11001100>>)
        {:ok, value} = get("flags")  # Returns computed AND result
        {:ok, value}
      end

  """
  @callback bit_and(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        bit_or("permission_flags", <<0b00001111>>)  # Set lower 4 bits
        {:ok, :ok}
      end

  """
  @callback bit_or(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        bit_xor("toggle_flags", <<0b10101010>>)  # Toggle alternate bits
        {:ok, :ok}
      end

  """
  @callback bit_xor(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        byte_min("earliest_date", "2024-01-01")
        {:ok, :ok}
      end

  """
  @callback byte_min(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        byte_max("latest_version", "1.2.3")
        {:ok, :ok}
      end

  """
  @callback byte_max(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        append_if_fits("log_buffer", "New log entry\\n")
        {:ok, :ok}
      end

  """
  @callback append_if_fits(Key.t(), value :: binary()) :: :ok

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
      transaction fn ->
        compare_and_clear("feature_flag", "enabled")
        {:ok, :ok}
      end

  """
  @callback compare_and_clear(Key.t(), expected :: binary()) :: :ok

  @doc """
  Performs an atomic operation on a key with the provided value.

  This is the low-level atomic operation interface. Most users should use the specific
  atomic operation functions (add, min, max, etc.) instead of calling this directly.

  ## Examples

      # These are equivalent:
      add("counter", 5)
      atomic(:add, "counter", <<5::64-little>>)

  """
  @callback atomic(
              operation ::
                :add
                | :min
                | :max
                | :bit_and
                | :bit_or
                | :bit_xor
                | :byte_min
                | :byte_max
                | :append_if_fits
                | :compare_and_clear,
              Key.t(),
              value :: binary()
            ) :: :ok

  defmacro __using__(opts) do
    cluster = Keyword.fetch!(opts, :cluster)

    quote do
      @behaviour Bedrock.Repo

      alias Bedrock.Internal.Repo
      alias Bedrock.Keyspace
      alias Bedrock.ToKeyRange

      @cluster unquote(cluster)

      # Expose cluster for internal use (e.g., nested transaction detection)
      @doc false
      def __cluster__, do: @cluster

      # Transaction Control

      @impl true
      def transact(fun, opts \\ []), do: Repo.transact(@cluster, __MODULE__, fun, opts)

      @impl true
      @spec rollback(reason :: term()) :: no_return()
      def rollback(reason), do: Repo.rollback(reason)

      @impl true
      def add_read_conflict_key(key), do: Repo.add_read_conflict_key(__MODULE__, key)

      @impl true
      def add_write_conflict_range(range) do
        {start_key, end_key} = ToKeyRange.to_key_range(range)
        Repo.add_write_conflict_range(__MODULE__, start_key, end_key)
      end

      # Read Operations

      @impl true
      def get(key) when is_binary(key), do: raw_get(key, [])

      @impl true
      def get(key, opts) when is_binary(key), do: raw_get(key, opts)

      @impl true
      def get(%Keyspace{} = keyspace, key), do: keyspace_get(keyspace, key, [])

      @impl true
      def get(%Keyspace{} = keyspace, key, opts), do: keyspace_get(keyspace, key, opts)

      @spec keyspace_get(Keyspace.t(), key :: term(), opts :: keyword()) :: nil | term()
      def keyspace_get(keyspace, key, opts) do
        case raw_get(Keyspace.pack(keyspace, key), opts) do
          nil -> nil
          value when is_nil(keyspace.value_encoding) -> value
          value -> keyspace.value_encoding.unpack(value)
        end
      end

      def raw_get(key) when not is_binary(key) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB")

      defp raw_get(key, opts), do: Repo.get(__MODULE__, key, opts)

      @impl true
      def select(key_selector, opts \\ []), do: Repo.select(__MODULE__, key_selector, opts)

      # Write Operations

      @impl true
      def clear(%Keyspace{} = keyspace, key), do: keyspace_clear(keyspace, key, [])

      @impl true
      def clear(%Keyspace{} = keyspace, key, opts), do: keyspace_clear(keyspace, key, opts)

      @impl true
      def clear(key), do: raw_clear(key, [])

      @impl true
      def clear(key, opts), do: raw_clear(key, opts)

      defp keyspace_clear(keyspace, key, opts), do: raw_clear(Keyspace.pack(keyspace, key), opts)

      defp raw_clear(key) when not is_binary(key) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB")

      defp raw_clear(key, opts), do: Repo.clear(__MODULE__, key, opts)

      @impl true
      def put(%Keyspace{} = keyspace, key, value), do: keyspace_put(keyspace, key, value, [])

      @impl true
      def put(%Keyspace{} = keyspace, key, value, opts), do: keyspace_put(keyspace, key, value, opts)

      @impl true
      def put(key, value), do: raw_put(key, value, [])

      @impl true
      def put(key, value, opts), do: raw_put(key, value, opts)

      defp raw_put(key, value, _opts)
           when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384 or byte_size(value) > 131_072,
           do: raise(ArgumentError, "key/value must be binary, key <= 16KiB, value <= 128KiB")

      defp raw_put(key, value, opts), do: Repo.put(__MODULE__, key, value, opts)

      defp keyspace_put(keyspace, key, value, opts) do
        packed_key = Keyspace.pack(keyspace, key)
        packed_value = if keyspace.value_encoding, do: keyspace.value_encoding.pack(value), else: value
        put(packed_key, packed_value, opts)
      end

      # Range Operations

      @impl true
      def get_range(range, opts \\ [])

      def get_range(%Keyspace{} = keyspace, opts), do: Keyspace.get_range_from_repo(keyspace, __MODULE__, opts)

      def get_range(range, opts) do
        {start_key, end_key} = ToKeyRange.to_key_range(range)
        Repo.get_range(__MODULE__, start_key, end_key, opts)
      end

      @impl true
      def clear_range(range, opts \\ [])

      def clear_range(range, opts) do
        {start_key, end_key} = ToKeyRange.to_key_range(range)
        Repo.clear_range(__MODULE__, start_key, end_key, opts)
      end

      # Atomic Operations

      @impl true
      def add(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def add(key, value), do: Repo.atomic(__MODULE__, :add, key, value)

      @impl true
      def min(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def min(key, value), do: Repo.atomic(__MODULE__, :min, key, value)

      @impl true
      def max(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def max(key, value), do: Repo.atomic(__MODULE__, :max, key, value)

      @impl true
      def bit_and(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def bit_and(key, value), do: Repo.atomic(__MODULE__, :bit_and, key, value)

      @impl true
      def bit_or(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def bit_or(key, value), do: Repo.atomic(__MODULE__, :bit_or, key, value)

      @impl true
      def bit_xor(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def bit_xor(key, value), do: Repo.atomic(__MODULE__, :bit_xor, key, value)

      @impl true
      def byte_min(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def byte_min(key, value), do: Repo.atomic(__MODULE__, :byte_min, key, value)

      @impl true
      def byte_max(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def byte_max(key, value), do: Repo.atomic(__MODULE__, :byte_max, key, value)

      @impl true
      def append_if_fits(key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def append_if_fits(key, value), do: Repo.atomic(__MODULE__, :append_if_fits, key, value)

      @impl true
      def compare_and_clear(key, expected)
          when not is_binary(key) or not is_binary(expected) or byte_size(key) > 16_384,
          do: raise(ArgumentError, "key must be binary and no larger than 16KiB, expected must be binary")

      def compare_and_clear(key, expected), do: Repo.atomic(__MODULE__, :compare_and_clear, key, expected)

      @impl true
      def atomic(op, _key, _value)
          when op not in [
                 :add,
                 :min,
                 :max,
                 :bit_and,
                 :bit_or,
                 :bit_xor,
                 :byte_min,
                 :byte_max,
                 :append_if_fits,
                 :compare_and_clear
               ], do: raise(ArgumentError, "Invalid operation; #{op}")

      def atomic(op, key, value) when not is_binary(key) or not is_binary(value) or byte_size(key) > 16_384,
        do: raise(ArgumentError, "key must be binary and no larger than 16KiB, value must be binary")

      def atomic(op, key, value), do: Repo.atomic(__MODULE__, op, key, value)
    end
  end
end
