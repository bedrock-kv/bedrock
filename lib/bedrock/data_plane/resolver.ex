defmodule Bedrock.DataPlane.Resolver do
  use Bedrock.Internal.GenServerApi

  @type ref :: GenServer.name()

  @type transaction :: {
          read_version :: Bedrock.version(),
          reads :: [Bedrock.key() | Bedrock.key_range()],
          writes :: [Bedrock.key() | Bedrock.key_range()]
        }

  @spec resolve_transactions(
          ref(),
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [transaction()]
        ) ::
          {:ok, aborted :: [index :: integer()]}
  def resolve_transactions(ref, last_version, commit_version, transaction_summaries) do
    call(
      ref,
      {:resolve_transactions, {last_version, commit_version}, transaction_summaries},
      :infinity
    )
  end
end
