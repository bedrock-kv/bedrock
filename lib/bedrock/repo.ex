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

      @spec range_fetch(t(), Subspace.t() | KeyRange.t()) :: {:ok, Enumerable.t(Bedrock.key_value())}
      @spec range_fetch(t(), Subspace.t() | KeyRange.t(), opts :: keyword()) :: {:ok, Enumerable.t(Bedrock.key_value())}

      def range_fetch(t, subspace_or_range, opts \\ [])
      def range_fetch(t, %Subspace{} = subspace, opts), do: range_fetch(t, Subspace.range(subspace), opts)
      def range_fetch(t, {start_key, end_key}, opts), do: range_fetch(t, start_key, end_key, opts)

      @spec range_fetch(t(), min_key :: binary(), max_key_ex :: binary(), opts :: keyword()) ::
              {:ok, Enumerable.t(Bedrock.key_value())}
      defdelegate range_fetch(t, start_key, end_key, opts), to: Repo

      @spec range_stream(t(), Subspace.t() | KeyRange.t(), opts :: keyword()) ::
              {:ok, Enumerable.t(Bedrock.key_value())}
      def range_stream(t, subspace_or_range, opts \\ [])
      def range_stream(t, %Subspace{} = subspace, opts), do: range_stream(t, Subspace.range(subspace), opts)
      def range_stream(t, {start_key, end_key}, opts), do: range_stream(t, start_key, end_key, opts)

      @spec range_stream(t(), min_key :: binary(), max_key_ex :: binary(), opts :: keyword()) ::
              {:ok, Enumerable.t(Bedrock.key_value())}
      defdelegate range_stream(t, start_key, end_key, opts), to: Repo

      @spec put(t(), key :: binary(), value :: binary()) :: t()
      def put(t, key, value) when is_binary(key) and is_binary(value), do: Repo.put(t, key, value)

      defdelegate commit(t, opts \\ []), to: Repo
      defdelegate rollback(t), to: Repo
    end
  end
end
