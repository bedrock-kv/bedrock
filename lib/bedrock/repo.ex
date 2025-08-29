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

      @cluster unquote(cluster)
      @key_codecs Map.merge(
                    Bedrock.Repo.builtin_key_codecs(),
                    Map.new(unquote(opts[:key_codecs] || []))
                  )
      @value_codecs Map.merge(
                      Bedrock.Repo.builtin_value_codecs(),
                      Map.new(unquote(opts[:value_codecs] || []))
                    )

      @opaque transaction :: Repo.transaction()

      defp key_codec(name), do: @key_codecs[name] || raise(ArgumentError, "Unknown key codec: #{inspect(name)}")

      defp value_codec(name), do: @value_codecs[name] || raise(ArgumentError, "Unknown value codec: #{inspect(name)}")

      @spec transaction(
              (transaction() -> result),
              opts :: [
                key_codec: atom() | module(),
                value_codec: atom() | module(),
                retry_count: non_neg_integer(),
                timeout_in_ms: Bedrock.timeout_in_ms()
              ]
            ) :: result
            when result: term()
      def transaction(fun, opts \\ []) do
        processed_opts =
          opts
          |> Keyword.put(:key_codec, key_codec(opts[:key_codec] || :default))
          |> Keyword.put(:value_codec, value_codec(opts[:value_codec] || :default))

        TransactionManager.transaction(@cluster, fun, processed_opts)
      end

      defdelegate fetch(t, key), to: Repo
      defdelegate fetch!(t, key), to: Repo
      defdelegate get(t, key), to: Repo
      defdelegate range_fetch(t, start_key, end_key, opts \\ []), to: Repo
      defdelegate range_stream(t, start_key, end_key, opts \\ []), to: Repo
      defdelegate put(t, key, value), to: Repo
      defdelegate commit(t, opts \\ []), to: Repo
      defdelegate rollback(t), to: Repo
    end
  end
end
