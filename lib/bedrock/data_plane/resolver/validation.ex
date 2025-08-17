defmodule Bedrock.DataPlane.Resolver.Validation do
  @moduledoc """
  Validation functions for resolver transactions.

  Provides validation logic for transaction summaries and ensures they conform
  to the expected format before processing in the resolver.
  """

  alias Bedrock.DataPlane.Transaction

  @spec check_transactions([Transaction.encoded()]) :: :ok | {:error, String.t()}
  def check_transactions(transactions) do
    cond do
      not is_list(transactions) ->
        {:error, "invalid transactions: expected list of transaction summaries, got #{inspect(transactions)}"}

      not Enum.all?(transactions, &valid_transaction_summary?/1) ->
        {:error,
         "invalid transaction format: all transactions must be transaction summaries with format {read_info | nil, write_keys}"}

      true ->
        :ok
    end
  end

  # Validation functions

  # Validates that a transaction is a proper transaction summary.
  # Transaction summaries have the format: {read_info | nil, write_keys}
  # where read_info is {version, keys} and write_keys is a list of keys.
  @spec valid_transaction_summary?(any()) :: boolean()
  defp valid_transaction_summary?({nil, write_keys}) when is_list(write_keys), do: true

  defp valid_transaction_summary?({{_version, read_keys}, write_keys}) when is_list(read_keys) and is_list(write_keys),
    do: true

  defp valid_transaction_summary?(_), do: false
end
