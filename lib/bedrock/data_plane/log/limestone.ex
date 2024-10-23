defmodule Bedrock.DataPlane.Log.Limestone do
  use Supervisor
  use Bedrock.Service.WorkerBehaviour

  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.LogController
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Limestone.Transactions

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
      states: ~w(starting locked ready)a,
      initial: :starting,
      transitions: %{
        starting: :locked,
        locked: [:locked, :ready],
        ready: :locked
      }
  end

  defmodule State do
    alias Bedrock.DataPlane.Log.Limestone.Subscriptions
    @type state :: :starting | :locked | :ready
    @type t :: %__MODULE__{
            state: state(),
            subscriber_liveness_timeout_in_s: integer(),
            id: String.t(),
            otp_name: atom(),
            transactions: Transactions.t(),
            controller: LogController.ref() | nil,
            epoch: Bedrock.epoch(),
            subscriptions: Subscriptions.t(),
            oldest_version: Bedrock.version() | nil,
            last_version: Bedrock.version() | nil,
            cluster_controller: ClusterController.ref() | nil
          }
    defstruct state: nil,
              subscriber_liveness_timeout_in_s: 60,
              id: nil,
              otp_name: nil,
              transactions: nil,
              controller: nil,
              epoch: nil,
              subscriptions: nil,
              oldest_version: nil,
              last_version: nil,
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
            last_version :: Bedrock.version(),
            last_durable_version :: Bedrock.version(),
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

    @spec update(t(), String.t(), Bedrock.version(), Bedrock.version()) :: :ok
    def update(t, id, last_version, last_durable_version) do
      :ets.insert(
        t,
        {id, last_version, last_durable_version, DateTime.utc_now() |> DateTime.to_unix()}
      )

      :ok
    end

    def minimum_durable_version(t, max_age_in_s) do
      :ets.select(t, match_last_seen_for_live_subscribers(max_age_in_s))
      |> case do
        [] -> nil
        last_durable_versions -> last_durable_versions |> Enum.min()
      end
    end

    defp match_last_seen_for_live_subscribers(max_age_in_s) do
      threshold_time = DateTime.utc_now() |> DateTime.to_unix() |> Kernel.-(max_age_in_s)

      [{{:_, :_, :"$3", :"$4"}, [{:>=, :"$4", threshold_time}], [:"$3"]}]
    end
  end

  defmodule Logic do
    @type t :: State.t()
    @type health :: Log.health()

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
         subscriptions: Subscriptions.new(),
         transactions: transactions,
         last_version: nil
       }}
    end

    @spec report_health_to_transaction_log_controller(t(), health()) :: :ok
    def report_health_to_transaction_log_controller(t, health),
      do: :ok = LogController.report_health(t.controller, t.id, health)

    @spec push(t(), Transaction.t(), prev_version :: Bedrock.version()) ::
            {:ok, t()} | {:error, :tx_out_of_order | :not_ready}
    def push(t, _transaction, _prev_version) when t.state not in [:ready, :locked],
      do: {:error, :not_ready}

    def push(t, _transaction, prev_version) when t.last_version != prev_version,
      do: {:error, :tx_out_of_order}

    def push(t, transaction, _prev_version) do
      Transactions.append!(t.transactions, transaction)
      {:ok, %{t | last_version: Transaction.version(transaction)}}
    end

    @spec lock_for_recovery(t(), ClusterController.ref(), Bedrock.epoch()) ::
            {:ok, t()} | {:error, :newer_epoch_exists | String.t()}
    def lock_for_recovery(t, cluster_controller, epoch) do
      State.transition_to(t, :locked, fn
        t when not is_nil(t.epoch) and t.epoch >= epoch ->
          {:halt, :newer_epoch_exists}

        t ->
          %{t | epoch: epoch, cluster_controller: cluster_controller}
      end)
    end

    @spec pull(
            t :: t(),
            last_version :: Bedrock.version(),
            count :: pos_integer(),
            opts :: [
              subscriber_id: String.t(),
              last_durable_version: Bedrock.version()
            ]
          ) ::
            {:ok, [] | [Transaction.t()]} | {:error, :not_ready | :tx_too_new}
    def pull(t, _last_version, _count, _opts)
        when t.state != :ready,
        do: {:error, :not_ready}

    def pull(t, last_version, _count, _opts)
        when last_version > t.last_version,
        do: {:error, :tx_too_new}

    def pull(t, last_version, count, subscriber_id: id = opts) do
      last_durable_version = opts[:last_durable_version] || :undefined

      t.transactions
      |> Transactions.get(last_version, count)
      |> case do
        [] ->
          t.subscriptions |> Subscriptions.update(id, last_version, last_durable_version)
          {:ok, []}

        transactions ->
          last_version = transactions |> List.last() |> Transaction.version()
          t.subscriptions |> Subscriptions.update(id, last_version, last_durable_version)
          {:ok, transactions}
      end
    end

    def pull(t, last_version, count, _opts) do
      {:ok,
       t.transactions
       |> Transactions.get(last_version, count)}
    end

    @spec info(State.t(), Log.fact_name() | [Log.fact_name()]) ::
            {:ok, term() | %{Log.fact_name() => term()}} | {:error, :unsupported_info}
    def info(%State{} = t, fact) when is_atom(fact), do: {:ok, gather_info(fact, t)}

    def info(%State{} = t, facts) when is_list(facts) do
      {:ok,
       facts
       |> Enum.reduce([], fn
         fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
       end)
       |> Map.new()}
    end

    defp supported_info, do: ~w[
      id
      kind
      minimum_durable_version
      oldest_version
      last_version
      otp_name
      pid
      state
      supported_info
    ]a

    @spec gather_info(Log.fact_name(), any()) :: term() | {:error, :unsupported}
    # Worker facts
    defp gather_info(:id, %{id: id}), do: id
    defp gather_info(:state, %{state: state}), do: state
    defp gather_info(:kind, _t), do: :log
    defp gather_info(:otp_name, %State{otp_name: otp_name}), do: otp_name
    defp gather_info(:pid, _), do: self()
    defp gather_info(:supported_info, _), do: supported_info()

    # Transaction Log facts
    defp gather_info(:minimum_durable_version, t) do
      Subscriptions.minimum_durable_version(t.subscriptions, t.subscriber_liveness_timeout_in_s)
    end

    defp gather_info(:oldest_version, t), do: t.oldest_version
    defp gather_info(:last_version, t), do: t.last_version

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

    def handle_call({:push, transaction, prev_version}, _from, %State{} = t) do
      Logic.push(t, transaction, prev_version)
      |> case do
        {:ok, t} -> {:reply, :ok, t}
        {:error, _reason} = error -> {:reply, error, t}
      end
    end

    def handle_call({:pull, last_version, count, opts}, _from, %State{} = t) do
      Logic.pull(t, last_version, count, opts)
      |> case do
        {:ok, []} -> {:reply, {:ok, []}, t}
        {:ok, transactions} -> {:reply, {:ok, transactions}, t}
      end
    end

    def handle_call({:lock_for_recovery, epoch}, controller, %State{} = t) do
      with {:ok, t} <- Logic.lock_for_recovery(t, controller, epoch),
           {:ok, info} <-
             Logic.info(t, [
               :last_version,
               :oldest_version,
               :minimum_durable_version
             ]) do
        {:reply, {:ok, self(), info}, t}
      else
        error -> {:reply, error, t}
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
  end
end
