defmodule Bedrock.DataPlane.Resolver do
  @moduledoc false

  alias Bedrock.DataPlane.Log

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @type read_info :: {version :: Bedrock.version(), keys :: [Bedrock.key() | Bedrock.key_range()]}

  @type transaction_summary :: {
          read_info :: read_info() | nil,
          write_keys :: [Bedrock.key() | Bedrock.key_range()]
        }

  @spec recover_from(
          ref(),
          lock_token :: binary(),
          logs_to_copy :: %{Log.id() => Log.ref()},
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          :ok
          | {:error, :timeout | :unavailable | :unknown}
  def recover_from(ref, lock_token, logs_to_copy, first_version, last_version),
    do:
      call(ref, {:recover_from, lock_token, logs_to_copy, first_version, last_version}, :infinity)

  @spec resolve_transactions(
          ref(),
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [transaction_summary()],
          opts :: [timeout: Bedrock.timeout_in_ms()]
        ) ::
          {:ok, aborted :: [transaction_index :: non_neg_integer()]}
          | {:error, :timeout | :unavailable | :unknown}
  def resolve_transactions(ref, last_version, commit_version, transaction_summaries, opts \\ []) do
    call(
      ref,
      {:resolve_transactions, {last_version, commit_version}, transaction_summaries},
      opts[:timeout] || :infinity
    )
  end
end
