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
        {:error, "invalid transactions: expected list of binary transactions, got #{inspect(transactions)}"}

      not Enum.all?(transactions, &valid_transaction_summary?/1) ->
        {:error, "invalid transaction format: all transactions must be binary"}

      true ->
        :ok
    end
  end

  # Validation functions

  # Validates that a transaction is a proper binary transaction.
  @spec valid_transaction_summary?(any()) :: boolean()
  defp valid_transaction_summary?(transaction) when is_binary(transaction), do: true
  defp valid_transaction_summary?(_), do: false
end
