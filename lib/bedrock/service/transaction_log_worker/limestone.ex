defmodule Bedrock.Service.TransactionLogWorker.Limestone do
  use Supervisor
  use Bedrock.Service.WorkerBehaviour

  alias Bedrock.Service.TransactionLogWorker.Limestone.SegmentRecycler
  alias Bedrock.Service.TransactionLogWorker.Limestone.Transactions
  alias Bedrock.Service.TransactionLogWorker.Limestone.TransactionReceiver

  defmodule Server do
    use GenServer

    defmodule State do
      @type t :: %__MODULE__{}
      defstruct [:id, :otp_name]
    end

    def child_spec(opts) do
      id = opts[:id] || raise "Missing :id option"
      otp_name = opts[:otp_name] || raise "Missing :otp_name option"

      %{
        id: __MODULE__,
        start:
          {GenServer, :start_link,
           [
             __MODULE__,
             {id, otp_name},
             [name: otp_name]
           ]},
        restart: :permanent
      }
    end

    @impl GenServer
    def init({id, otp_name}) do
      {:ok, %State{id: id, otp_name: otp_name}}
    end

    @impl GenServer
    def handle_call({:info, fact_names}, _from, t) do
      {:reply, t |> info(fact_names), t}
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
             path,
             opts[:minimum_available] || 3,
             opts[:segment_size] || 64 * 1024 * 1024,
             controller
           }
         ]},
      type: :supervisor
    }
  end

  @impl Supervisor
  def init({otp_name, id, path, minimum_available, segment_size, controller}) do
    transactions = Transactions.new(:"#{otp_name}_transactions")

    recycler_name = :"#{otp_name}_recycler"
    transaction_receiver_name = :"#{otp_name}_receiver"

    children =
      [
        {SegmentRecycler,
         [
           minimum_available: minimum_available,
           segment_size: segment_size,
           path: path,
           otp_name: recycler_name
         ]},
        {TransactionReceiver,
         [
           transactions: transactions,
           recycler: recycler_name,
           controller: controller,
           otp_name: transaction_receiver_name
         ]},
        {Server,
         [
           id: id,
           otp_name: otp_name
         ]}
      ]

    # Controller.report_worker_health(controller, id, :ok)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
