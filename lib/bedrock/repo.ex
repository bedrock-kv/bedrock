defmodule Bedrock.Repo do
  alias Bedrock.KeyCodec.BinaryKeyCodec
  alias Bedrock.ValueCodec.BinaryValueCodec

  @spec builtin_key_codecs() :: %{(:default | :binary | :tuple) => module()}
  def builtin_key_codecs, do: %{default: BinaryKeyCodec, binary: BinaryKeyCodec, tuple: Bedrock.KeyCodec.TupleKeyCodec}

  @spec builtin_value_codecs() :: %{(:default | :raw | :bert) => module()}
  def builtin_value_codecs,
    do: %{default: BinaryValueCodec, raw: BinaryValueCodec, bert: Bedrock.ValueCodec.BertValueCodec}

  defmacro __using__(opts) do
    cluster = Keyword.fetch!(opts, :cluster)

    quote do
      alias Bedrock.Internal.Repo
      alias Bedrock.Internal.TransactionManager
      alias Bedrock.Subspace

      @cluster unquote(cluster)

      @opaque t :: Repo.transaction()

      @spec transaction(
              (t() -> result),
              opts :: [
                retry_count: non_neg_integer(),
                timeout_in_ms: Bedrock.timeout_in_ms()
              ]
            ) :: result
            when result: t()
      def transaction(fun, opts \\ []), do: TransactionManager.transaction(@cluster, fun, opts)

      defdelegate fetch(t, key), to: Repo
      defdelegate fetch!(t, key), to: Repo
      defdelegate get(t, key), to: Repo

      @doc """
      Lazy range query that returns an enumerable stream of key-value pairs.

      Like FoundationDB's get_range(), this function returns an Enumerable that lazily
      fetches results as needed. For eager collection, pipe to Enum.to_list/1.

      ## Examples

          # Lazy iteration (memory efficient)
          transaction fn tx ->
            tx
            |> Repo.range(start_key, end_key, limit: 100)
            |> Enum.take(10)  # Only fetches what's needed
          end

          # Collect all results
          transaction fn tx ->
            results =
              tx
              |> Repo.range(start_key, end_key)
              |> Enum.to_list()
          end

      """
      @spec range(t(), Subspace.t() | KeyRange.t(), opts :: keyword()) :: Enumerable.t(Bedrock.key_value())
      def range(t, subspace_or_range, opts \\ [])
      def range(t, %Subspace{} = subspace, opts), do: range(t, Subspace.range(subspace), opts)
      def range(t, {start_key, end_key}, opts), do: range(t, start_key, end_key, opts)

      @spec range(t(), min_key :: binary(), max_key_ex :: binary(), opts :: keyword()) ::
              Enumerable.t(Bedrock.key_value())
      defdelegate range(t, start_key, end_key, opts), to: Repo

      @spec put(t(), key :: binary(), value :: binary()) :: t()
      def put(t, key, value) when is_binary(key) and is_binary(value), do: Repo.put(t, key, value)

      defdelegate commit(t, opts \\ []), to: Repo
      defdelegate rollback(t), to: Repo
    end
  end
end
