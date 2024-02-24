defmodule Bedrock.Raft.Log.InMemoryLog do
  @moduledoc """
  An implementation of the transaction log that stores transactions in memory,
  and makes no guarantees about durability or persistence. This is useful for
  testing and development, but should not be used in production.
  """

  defstruct ~w[
    format
    transactions
    last_commit
  ]a

  @type t :: %__MODULE__{}

  @doc """
  Create a new in-memory log. The `format` argument can be `:tuple` or
  `:binary`, and determines the format of the transactions in the log.
  """
  @spec new(:tuple | :binary) :: t()
  def new(format \\ :tuple),
    do: %__MODULE__{
      format: format,
      transactions: :ets.new(:in_memory_log, [:ordered_set])
    }

  defimpl Bedrock.Raft.Log do
    alias Bedrock.Raft.TransactionID

    @initial_transaction_id TransactionID.new(0, 0)
    @binary_initial_transaction_id TransactionID.encode(@initial_transaction_id)

    @impl Bedrock.Raft.Log
    def append_transactions(%{format: :tuple} = t, @initial_transaction_id, transactions) do
      true = :ets.insert_new(t.transactions, transactions |> Enum.map(&{&1, :undefined}))
      {:ok, t}
    end

    def append_transactions(%{format: :binary} = t, @binary_initial_transaction_id, transactions) do
      true = :ets.insert_new(t.transactions, transactions |> Enum.map(&{&1, :undefined}))
      {:ok, t}
    end

    def append_transactions(t, prev, transactions)
        when (t.format == :tuple and is_tuple(prev)) or
               (t.format == :binary and is_binary(prev)) do
      :ets.lookup(t.transactions, prev)
      |> case do
        [{^prev, _}] ->
          true = :ets.insert_new(t.transactions, transactions |> Enum.map(&{&1, :undefined}))
          {:ok, t}

        [] ->
          {:error, :prev_transaction_not_found}
      end
    end

    @impl Bedrock.Raft.Log
    def initial_transaction_id(%{format: :tuple}), do: @initial_transaction_id
    def initial_transaction_id(%{format: :binary}), do: @binary_initial_transaction_id

    @impl Bedrock.Raft.Log
    def commit_up_to(t, transaction)
        when (t.format == :tuple and is_tuple(transaction)) or
               (t.format == :binary and is_binary(transaction)) do
      {:ok, %{t | last_commit: transaction}}
    end

    @impl Bedrock.Raft.Log
    def newest_transaction_id(t) do
      :ets.last(t.transactions)
      |> case do
        :"$end_of_table" ->
          case t.format do
            :binary -> @binary_initial_transaction_id
            :tuple -> @initial_transaction_id
          end

        transaction ->
          transaction
      end
    end

    @impl Bedrock.Raft.Log
    def newest_safe_transaction_id(t), do: t.last_commit || initial_transaction_id(t)

    @impl Bedrock.Raft.Log
    def has_transaction_id?(%{format: :tuple}, @initial_transaction_id), do: true
    def has_transaction_id?(%{format: :binary}, @binary_initial_transaction_id), do: true
    def has_transaction_id?(t, transaction), do: :ets.member(t.transactions, transaction)

    @impl Bedrock.Raft.Log
    def transactions_to(t, :newest),
      do: transactions_from(t, initial_transaction_id(t), newest_transaction_id(t))

    def transactions_to(t, :newest_safe),
      do: transactions_from(t, initial_transaction_id(t), newest_safe_transaction_id(t))

    @impl Bedrock.Raft.Log
    def transactions_from(t, from, :newest),
      do: transactions_from(t, from, newest_transaction_id(t))

    def transactions_from(t, from, :newest_safe),
      do: transactions_from(t, from, newest_safe_transaction_id(t))

    def transactions_from(t, from, to) do
      :ets.select(t.transactions, match_gte_lte(from, to))
      |> case do
        [^from | transactions] ->
          transactions

        transactions
        when (t.format == :tuple and from == @initial_transaction_id) or
               (t.format == :binary and from == @binary_initial_transaction_id) ->
          transactions

        [] ->
          []
      end
    end

    defp match_gte_lte(gte, lte) do
      [{{:"$1", :_}, [{:>=, :"$1", {:const, gte}}, {:"=<", :"$1", {:const, lte}}], [:"$1"]}]
    end
  end
end
