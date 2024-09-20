defmodule Bedrock.Service.TransactionLogWorker.Limestone do
  use Bedrock.Service.WorkerBehaviour
  use Supervisor

  alias Bedrock.Service.Controller
  alias Bedrock.Service.TransactionLogWorker.Limestone.SegmentRecycler
  alias Bedrock.Service.TransactionLogWorker.Limestone.Transactions
  alias Bedrock.Service.TransactionLogWorker.Limestone.TransactionReceiver

  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id)
    path = Keyword.fetch!(opts, :path)
    controller = Keyword.fetch!(opts, :controller)

    %{
      id: __MODULE__,
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
           },
           [name: otp_name]
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
         ]}
      ]

    Controller.report_worker_health(controller, id, :ok)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
