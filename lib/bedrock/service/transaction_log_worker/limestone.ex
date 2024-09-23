defmodule Bedrock.Service.TransactionLogWorker.Limestone do
  use Supervisor
  use Bedrock.Service.WorkerBehaviour

  alias Bedrock.Service.TransactionLogWorker.Limestone.SegmentRecycler
  alias Bedrock.Service.TransactionLogWorker.Limestone.Transactions
  alias Bedrock.Service.TransactionLogWorker.Limestone.TransactionReceiver

  defmodule Server do
    use GenServer

    def child_spec(opts) do
      otp_name = opts[:otp_name] || raise "Missing :otp_name option"

      %{
        id: __MODULE__,
        start:
          {GenServer, :start_link,
           [
             __MODULE__,
             opts,
             [name: otp_name]
           ]},
        restart: :permanent
      }
    end

    @impl GenServer
    def init(opts) do
      IO.inspect(opts)
      {:ok, :nostate}
    end

    @impl GenServer
    def handle_call({:info, _fact_names}, _from, state) do
      IO.inspect("asdasdasd!")
      {:reply, {:ok, []}, state}
    end
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
  def init({otp_name, path, minimum_available, segment_size, controller}) do
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
           otp_name: otp_name
         ]}
      ]

    # Controller.report_worker_health(controller, id, :ok)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
