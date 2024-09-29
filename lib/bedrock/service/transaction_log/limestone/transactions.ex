defmodule Bedrock.Service.TransactionLog.Limestone.Transactions do
  use Bedrock.Cluster, :types

  defstruct ~w[ets]a
  @type t :: %__MODULE__{}

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.TransactionLog.Limestone.Segment

  @spec new(atom()) :: t()
  def new(name) do
    ets =
      :ets.new(name, [
        :ordered_set,
        :named_table,
        :public,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])

    %__MODULE__{ets: ets}
  end

  @spec delete(t()) :: :ok
  def delete(t) do
    true = :ets.delete(t.ets)
    :ok
  end

  @spec get(t(), version(), count :: pos_integer()) :: [transaction()]
  def get(t, version, count) do
    :ets.select(t.ets, match_value_for_key_with_version_gt(version), count)
    |> case do
      {transactions, _continuation} ->
        transactions

      :"$end_of_table" ->
        []
    end
  end

  defp match_value_for_key_with_version_gt(version),
    do: [{{:"$1", :_}, [{:>, :"$1", version}], [:"$_"]}]

  @doc """
  Load all transactions from the given segment into the transaction log.
  """
  @spec from_segment(t(), Segment.t()) :: :ok
  def from_segment(t, segment) do
    segment
    |> Segment.stream!()
    |> into_ets(t.ets)
  end

  @doc """
  Load all transactions from the given segment into the transaction log starting
  at the given version, exclusive.
  """
  @spec from_segment(t(), Segment.t(), at_version :: version()) :: :ok
  def from_segment(t, segment, at_version) do
    segment
    |> Segment.stream!()
    |> Stream.drop_while(&(Transaction.version(&1) <= at_version))
    |> into_ets(t.ets)
  end

  @spec into_ets(Enumerable.t(), :ets.tid()) :: :ok
  defp into_ets(transaction_stream, ets) do
    true = :ets.insert(ets, transaction_stream |> Enum.to_list())
    :ok
  end

  @doc """
  Append one or more transactions to the transaction log. It will raise if any
  of the given transactions are already in the log.
  """
  @spec append!(t(), transaction() | [transaction()]) :: :ok
  def append!(t, transaction) when is_tuple(transaction) or is_list(transaction) do
    :ets.insert_new(t.ets, transaction) || raise "duplicate transaction"
    :ok
  end
end
