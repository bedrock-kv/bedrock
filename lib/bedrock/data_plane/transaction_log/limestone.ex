defmodule Bedrock.DataPlane.TransactionLog.Limestone do
  use Supervisor
  use Bedrock.Service.WorkerBehaviour

  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.TransactionLogController
  alias Bedrock.DataPlane.TransactionLog
  alias Bedrock.DataPlane.TransactionLog.Limestone.Transactions

  @doc false
  @spec child_spec(opts :: keyword() | []) :: Supervisor.child_spec()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id)
    path = Keyword.fetch!(opts, :path)
    controller = Keyword.fetch!(opts, :controller)

    %{
      id: {__MODULE__, id},
      start:
        {Supervisor, :start_link,
         [
           __MODULE__,
           {
             otp_name,
             id,
             controller,
             path,
             opts[:min_spare_segments] || 3,
             opts[:max_spare_segments] || 5,
             opts[:segment_size] || 64 * 1024 * 1024
           }
         ]},
      type: :supervisor
    }
  end

  @impl Supervisor
  def init(
        {otp_name, id, controller, _path, _min_spare_segments, _max_spare_segments, _segment_size}
      ) do
    transactions = Transactions.new(:"#{otp_name}_transactions")

    children =
      [
        {__MODULE__.Server,
         [
           id: id,
           otp_name: otp_name,
           controller: controller,
           transactions: transactions
         ]}
      ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defmodule StateMachine do
    use Gearbox,
      states: ~w(starting waiting locked ready)a,
      initial: :starting,
      transitions: %{
        starting: :waiting,
        waiting: :locked,
        locked: [:locked, :ready]
      }
  end

  defmodule State do
    @type state :: :starting | :waiting | :locked | :ready
    @type t :: %__MODULE__{
            state: state(),
            subscriber_liveness_timeout_in_s: integer(),
            id: String.t(),
            otp_name: atom(),
            transactions: Transactions.t(),
            controller: TransactionLogController.t() | nil,
            epoch: Bedrock.epoch() | nil,
            last_tx_id: Transaction.version() | :undefined,
            cluster_controller: ClusterController.t() | nil
          }
    defstruct state: nil,
              subscriber_liveness_timeout_in_s: 60,
              id: nil,
              otp_name: nil,
              transactions: nil,
              controller: nil,
              epoch: nil,
              last_tx_id: nil,
              cluster_controller: nil

    @type transition_fn :: (t() -> t())

    @spec transition_to(t(), state(), transition_fn()) :: {:ok, t()} | {:error, any()}
    def transition_to(t, new_state, transition_fn) do
      Gearbox.transition(t, StateMachine, new_state)
      |> case do
        {:ok, new_t} ->
          transition_fn.(new_t)
          |> case do
            {:halt, reason} -> {:error, reason}
            new_t -> {:ok, new_t}
          end

        error ->
          error
      end
    end
  end

  defmodule Subscriptions do
    @type t :: :ets.table()
    @type subscription :: {
            id :: String.t(),
            last_tx_id :: Transaction.version(),
            last_durable_tx_id :: Transaction.version(),
            last_seen_at :: integer()
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
      :ets.insert(
        t,
        {id, last_tx_id, last_durable_tx_id, DateTime.utc_now() |> DateTime.to_unix()}
      )

      :ok
    end

    def minimum_durable_tx_id(t, max_age_in_s) do
      :ets.select(t, match_last_seen_for_live_subscribers(max_age_in_s))
      |> case do
        [] -> :unknown
        last_durable_tx_ids -> last_durable_tx_ids |> Enum.min()
      end
    end

    defp match_last_seen_for_live_subscribers(max_age_in_s),
      do: [
        {{:_, :_, :"$3", :"$4"},
         [
           {:>=, :"$4",
            {:constant, DateTime.utc_now() |> DateTime.to_unix() |> Kernel.-(max_age_in_s)}}
         ], [:"$3"]}
      ]
  end

  defmodule Logic do
    @type t :: State.t()
    @type fact_name :: TransactionLog.fact_name()
    @type health :: TransactionLog.health()

    @spec startup(
            id :: String.t(),
            otp_name :: atom(),
            controller :: pid(),
            Transactions.t()
          ) :: {:ok, t()} | {:error, term()}
    def startup(id, otp_name, controller, transactions) do
      {:ok,
       %State{
         state: :starting,
         id: id,
         otp_name: otp_name,
         controller: controller,
         transactions: transactions,
         last_tx_id: :undefined
       }}
    end

    @spec report_health_to_transaction_log_controller(t(), health()) :: :ok
    def report_health_to_transaction_log_controller(t, health),
      do: :ok = TransactionLogController.report_health(t.controller, t.id, health)

    @spec push(t(), Transaction.t(), prev_tx_id :: Transaction.version()) ::
            {:ok, t()} | {:error, :tx_out_of_order | :not_ready}
    def push(t, _transaction, _prev_tx_id) when t.state not in [:ready, :locked],
      do: {:error, :not_ready}

    def push(t, _transaction, prev_tx_id) when t.last_tx_id != prev_tx_id,
      do: {:error, :tx_out_of_order}

    def push(t, transaction, _prev_tx_id) do
      Transactions.append!(t.transactions, transaction)
      {:ok, %{t | last_tx_id: Transaction.version(transaction)}}
    end

    @spec lock(t(), ClusterController.service(), Bedrock.epoch()) ::
            {:ok, t()} | {:error, :epoch_too_old | String.t()}
    def lock(t, cluster_controller, epoch) do
      State.transition_to(t, :locked, fn
        t when t.epoch >= epoch ->
          {:halt, :epoch_too_old}

        t ->
          %{t | epoch: epoch, cluster_controller: cluster_controller}
      end)
    end

    @spec report_lock_complete_to_cluster_controller(t()) :: :ok
    def report_lock_complete_to_cluster_controller(t) do
      {:ok, info} = Logic.info(t, [:last_tx_id, :minimum_durable_tx_id])

      :ok =
        ClusterController.report_transaction_log_lock_complete(
          t.cluster_controller,
          t.id,
          info
        )
    end

    @spec pull(
            t :: t(),
            last_tx_id :: Transaction.version(),
            count :: pos_integer(),
            opts :: [
              subscriber_id: String.t(),
              last_durable_tx_id: Transaction.version()
            ]
          ) ::
            {:ok, [] | [Transaction.t()]} | {:error, :not_ready | :tx_too_new}
    def pull(t, _last_tx_id, _count, _opts)
        when t.state != :ready,
        do: {:error, :not_ready}

    def pull(t, last_tx_id, _count, _opts)
        when last_tx_id > t.last_tx_id,
        do: {:error, :tx_too_new}

    def pull(t, last_tx_id, count, subscriber_id: id = opts) do
      last_durable_tx_id = opts[:last_durable_tx_id] || :undefined

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

    def pull(t, last_tx_id, count, _opts) do
      {:ok,
       t.transactions
       |> Transactions.get(last_tx_id, count)}
    end

    @spec info(t(), fact_name() | [fact_name()]) :: {:ok, any()} | {:error, :unsupported_info}
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
      minimum_durable_tx_id
      otp_name
      pid
      state
      supported_info
    ]a

    @spec gather_info(fact_name(), any()) :: term() | {:error, :unsupported}
    # Worker facts
    defp gather_info(:id, %{id: id}), do: id
    defp gather_info(:state, %{state: state}), do: state
    defp gather_info(:kind, _t), do: :transaction_log
    defp gather_info(:otp_name, %State{otp_name: otp_name}), do: otp_name
    defp gather_info(:pid, _), do: self()
    defp gather_info(:supported_info, _), do: supported_info()

    # Transaction Log facts
    defp gather_info(:minimum_durable_tx_id, t),
      do: Subscriptions.minimum_durable_tx_id(t.subscriptions, t.subscriber_liveness_timeout_in_s)

    # Everything else...
    defp gather_info(_, _), do: {:error, :unsupported}
  end

  defmodule Server do
    use GenServer

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
             __MODULE__,
             {id, otp_name, controller, transactions},
             [name: otp_name]
           ]}
      }
    end

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

    def handle_call({:push, transaction, prev_tx_id}, _from, %State{} = t) do
      Logic.push(t, transaction, prev_tx_id)
      |> case do
        {:ok, t} -> {:reply, :ok, t}
        {:error, _reason} = error -> {:reply, error, t}
      end
    end

    def handle_call({:pull, last_tx_id, count, opts}, _from, %State{} = t) do
      Logic.pull(t, last_tx_id, count, opts)
      |> case do
        {:ok, []} -> {:reply, {:ok, []}, t}
        {:ok, transactions} -> {:reply, {:ok, transactions}, t}
      end
    end

    @impl GenServer
    def handle_cast({:lock, controller, epoch}, %State{} = t) do
      Logic.lock(t, controller, epoch)
      |> case do
        {:ok, t} -> {:noreply, t, {:continue, :report_lock_complete_to_cluster_controller}}
        _error -> {:noreply, t}
      end
    end

    @impl GenServer
    def handle_continue(:finish_startup, {id, otp_name, controller, transactions}) do
      Logic.startup(id, otp_name, controller, transactions)
      |> case do
        {:ok, t} -> {:noreply, t, {:continue, :report_health_to_controller}}
      end
    end

    def handle_continue(:report_health_to_controller, %State{} = t) do
      :ok = Logic.report_health_to_transaction_log_controller(t, :ok)
      {:noreply, t}
    end

    def handle_continue(:report_lock_complete_to_cluster_controller, t) do
      :ok = Logic.report_lock_complete_to_cluster_controller(t)
      {:noreply, t}
    end
  end
end
