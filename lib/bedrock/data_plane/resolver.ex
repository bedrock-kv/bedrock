defmodule Bedrock.DataPlane.Resolver do
  alias Bedrock.DataPlane.Log

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.name()

  @type read_info :: {version :: Bedrock.version(), keys :: [Bedrock.key() | Bedrock.key_range()]}

  @type transaction :: {
          read_info :: read_info() | nil,
          write_keys :: [Bedrock.key() | Bedrock.key_range()]
        }

  @spec recover_from(
          ref(),
          logs_to_copy :: %{Log.id() => Log.ref()},
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          :ok
          | {:error, :timeout}
          | {:error, :unavailable}
  def recover_from(ref, logs_to_copy, first_version, last_version),
    do: call(ref, {:recover_from, logs_to_copy, first_version, last_version}, :infinity)

  @spec resolve_transactions(
          ref(),
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [transaction()],
          opts :: [timeout: :infinity | non_neg_integer()]
        ) ::
          {:ok, aborted :: [index :: integer()]}
          | {:error, :timeout}
          | {:error, :unavailable}
  def resolve_transactions(ref, last_version, commit_version, transaction_summaries, opts \\ []) do
    call(
      ref,
      {:resolve_transactions, {last_version, commit_version}, transaction_summaries},
      opts[:timeout] || :infinity
    )
  end
end
