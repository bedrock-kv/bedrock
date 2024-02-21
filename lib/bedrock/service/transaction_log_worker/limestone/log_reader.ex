defmodule Bedrock.Service.TransactionLogWorker.Limestone.LogReader do
  use GenStage
  use Bedrock.Cluster, :types

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.TransactionLogWorker.Limestone.Transactions

  defstruct ~w[transactions demand version]a

  @type t :: %__MODULE__{
          transactions: Transactions.t(),
          demand: non_neg_integer(),
          version: version()
        }

  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)
    version = Keyword.fetch!(opts, :version)
    transactions = opts |> Keyword.fetch!(:transactions)

    %{
      id: __MODULE__,
      start:
        {GenStage, :start_link,
         [
           __MODULE__,
           %{
             transactions: transactions,
             version: version
           },
           [name: name]
         ]},
      restart: :transient
    }
  end

  @impl GenStage
  def init(args) do
    {:producer, %__MODULE__{transactions: args.transactions}}
  end

  @impl GenStage
  def handle_demand(demand, state) do
    {state, transactions} =
      state
      |> increase_demand_by(demand)
      |> attempt_to_satify_demand()

    {:noreply, transactions, state}
  end

  @spec increase_demand_by(t(), non_neg_integer()) :: t()
  def increase_demand_by(state, demand), do: %{state | demand: state.demand + demand}

  @spec decrease_demand_by_one(t()) :: t()
  def decrease_demand_by_one(%{demand: 0}), do: raise("Demand is already zero.")
  def decrease_demand_by_one(state), do: %{state | demand: state.demand - 1}

  @spec update_version(t(), version()) :: t()
  def update_version(state, version), do: %{state | version: version}

  @spec attempt_to_satify_demand(t()) :: {t(), [transaction()]}
  def attempt_to_satify_demand(%{demand: 0} = state), do: {state, []}

  def attempt_to_satify_demand(state) do
    transactions =
      state.transactions
      |> Transactions.get(state.version, state.demand)

    state =
      transactions
      |> Enum.reduce(state, fn t, state ->
        state
        |> decrease_demand_by_one()
        |> update_version(Transaction.version(t))
      end)

    {state, transactions}
  end
end
