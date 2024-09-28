defmodule Bedrock.Service.TransactionLogWorker.Limestone.Server do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Controller
  alias Bedrock.Service.TransactionLogWorker.Limestone.SegmentRecycler
  alias Bedrock.Service.TransactionLogWorker.Limestone.Transactions

  def child_spec(opts) do
    id = Keyword.fetch!(opts, :id)
    otp_name = Keyword.fetch!(opts, :otp_name)
    controller = Keyword.fetch!(opts, :controller)
    transactions = Keyword.fetch!(opts, :transactions)

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__.Server,
           {id, otp_name, controller, transactions},
           [name: otp_name]
         ]}
    }
  end

  defmodule State do
    @type t :: %__MODULE__{
            mode: :waiting | :locked | :ready,
            id: String.t(),
            otp_name: atom(),
            transactions: Transactions.t(),
            controller: Controller.t(),
            last_tx_id: Transaction.version() | :undefined,
            cluster_controller: ClusterController.t() | nil
          }
    defstruct mode: nil,
              id: nil,
              otp_name: nil,
              transactions: nil,
              controller: nil,
              last_tx_id: nil,
              cluster_controller: nil
  end

  defmodule Subscriptions do
    @type t :: :ets.table()
    @type subscription :: {
            id :: String.t(),
            last_tx_id :: Transaction.version(),
            last_durable_tx_id :: Transaction.version(),
            last_seen :: DateTime.t()
          }

    def new, do: :ets.new(:subscribers, [:ordered_set])

    @spec all(t()) :: {:ok, [subscription()]}
    def all(t), do: {:ok, :ets.tab2list(t)}

    @spec lookup(t(), String.t()) :: {:ok, subscription()} | {:error, :not_found}
    def lookup(t, id) do
      :ets.lookup(t, id)
      |> case do
        [] -> {:error, :not_found}
        [sub] -> {:ok, sub}
      end
    end

    @spec update(t(), String.t(), Transaction.version(), Transaction.version()) :: :ok
    def update(t, id, last_tx_id, last_durable_tx_id) do
      :ets.insert(t, {id, last_tx_id, last_durable_tx_id, DateTime.utc_now()})
      :ok
    end
  end

  defmodule Logic do
    @spec startup(
            id :: String.t(),
            otp_name :: atom(),
            controller :: pid(),
            Transactions.t()
          ) :: {:ok, State.t()} | {:error, term()}
    def startup(id, otp_name, controller, transactions) do
      {:ok,
       %State{
         mode: :waiting,
         id: id,
         otp_name: otp_name,
         controller: controller,
         transactions: transactions,
         last_tx_id: :undefined
       }}
    end

    @spec apply_transaction(State.t(), Transaction.t(), prev_tx_id :: Transaction.version()) ::
            {:ok, State.t()} | {:error, :out_of_order | :not_ready}
    def apply_transaction(t, _transaction, prev_tx_id) when t.last_tx_id != prev_tx_id,
      do: {:error, :out_of_order}

    def apply_transaction(t, _transaction, _prev_tx_id) when t.mode != :ready,
      do: {:error, :not_ready}

    def apply_transaction(t, transaction, _prev_tx_id) do
      Transactions.append!(t.transactions, transaction)
      {:ok, %{t | last_tx_id: Transaction.version(transaction)}}
    end

    def request_lock(t, _controller, epoch) when t.epoch >= epoch, do: {:error, :epoch_too_old}

    def request_lock(t, cluster_controller, epoch),
      do: {:ok, %{t | epoch: epoch, cluster_controller: cluster_controller, state: :locked}}

    @spec pull_transactions(
            t :: State.t(),
            id :: String.t(),
            last_tx_id :: Transaction.version(),
            count :: pos_integer(),
            last_durable_tx_id :: Transaction.version()
          ) ::
            {:ok, [] | [Transaction.t()]} | {:error, :transaction_too_new}
    def pull_transactions(t, _id, last_tx_id, _count, _last_durable_tx_id)
        when last_tx_id > t.last_tx_id,
        do: {:error, :transaction_too_new}

    def pull_transactions(t, id, last_tx_id, count, last_durable_tx_id) do
      t.transactions
      |> Transactions.get(last_tx_id, count)
      |> case do
        [] ->
          t.subscriptions |> Subscriptions.update(id, last_tx_id, last_durable_tx_id)
          {:ok, []}

        transactions ->
          last_tx_id = transactions |> List.last() |> Transaction.version()
          t.subscriptions |> Subscriptions.update(id, last_tx_id, last_durable_tx_id)
          {:ok, transactions}
      end
    end

    @spec info(State.t(), atom() | [atom()]) :: {:ok, any()} | {:error, :unsupported_info}
    def info(%State{} = t, fact) when is_atom(fact), do: {:ok, gather_info(fact, t)}

    def info(%State{} = t, facts) when is_list(facts) do
      {:ok,
       facts
       |> Enum.reduce([], fn
         fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
       end)}
    end

    defp supported_info, do: ~w[
      id
      kind
      pid
      otp_name
      supported_info
    ]a

    @spec gather_info(fact_name :: atom(), any()) :: term() | {:error, :unsupported}
    defp gather_info(:id, %{id: id}), do: id
    defp gather_info(:kind, _t), do: :transaction_log
    defp gather_info(:otp_name, %State{otp_name: otp_name}), do: otp_name
    defp gather_info(:pid, _), do: self()
    defp gather_info(:supported_info, _), do: supported_info()
    defp gather_info(_, _), do: {:error, :unsupported}
  end

  defmodule Server do
    use GenServer

    @impl GenServer
    def init(args),
      # We use a continuation here to ensure that the controller isn't blocked
      # waiting for the worker to finish it's startup sequence (which could take
      # a few seconds or longer if the transaction log is large.) The
      # controller will be notified when the worker is ready to accept requests.
      do: {:ok, args, {:continue, :finish_startup}}

    @impl GenServer
    def handle_call({:info, fact_names}, _from, %State{} = t),
      do: {:reply, Logic.info(t, fact_names), t}

    def handle_call({:apply_transaction, transaction, prev_tx_id}, _from, %State{} = t) do
      Logic.apply_transaction(t, transaction, prev_tx_id)
      |> case do
        {:ok, t} -> {:reply, :ok, t}
        {:error, _reason} = error -> {:reply, error, t}
      end
    end

    def handle_call(
          {:pull_transactions, id, last_tx_id, count, last_durable_tx_id},
          _from,
          %State{} = t
        ) do
      Logic.pull_transactions(t, id, last_tx_id, count, last_durable_tx_id)
      |> case do
        {:ok, []} -> {:reply, {:ok, []}, t}
        {:ok, transactions} -> {:reply, {:ok, transactions}, t}
      end
    end

    @impl GenServer
    def handle_info({:request_lock, controller, epoch}, %State{} = t) do
      Logic.request_lock(t, controller, epoch)
      |> case do
        {:ok, t} -> {:noreply, t, {:continue, :finish_lock_request}}
        _error -> {:noreply, t}
      end
    end

    @impl GenServer
    def handle_continue(:finish_startup, {id, otp_name, controller, transactions}) do
      Logic.startup(id, otp_name, controller, transactions)
      |> case do
        {:ok, t} ->
          {:noreply, t, {:continue, :report_health_to_controller}}
          # {:error, reason} -> {:stop, reason, :nostate}
      end
    end

    def handle_continue(:report_health_to_controller, %State{} = t) do
      :ok = Controller.report_worker_health(t.controller, t.id, :ok)
      {:noreply, t}
    end

    def handle_continue(:finish_lock_request, t) do
      #      ClusterController.notify_lock_request_complete(t.cluster_controller, t.id)
      {:noreply, t}
    end
  end
end
