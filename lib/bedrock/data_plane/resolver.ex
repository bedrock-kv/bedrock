defmodule Bedrock.DataPlane.Resolver do
  alias Bedrock.DataPlane.Log

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.name()

  @type transaction :: {
          read_version :: Bedrock.version(),
          read_keys :: [Bedrock.key() | Bedrock.key_range()],
          write_keys :: [Bedrock.key() | Bedrock.key_range()]
        }

  @spec recover_from(
          ref(),
          source_log :: Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          :ok
          | {:error, :timeout}
          | {:error, :unavailable}
  def recover_from(ref, source_log, first_version, last_version),
    do: call(ref, {:recover_from, source_log, first_version, last_version}, :infinity)

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
