defmodule Bedrock.Service.TransactionLogWorker.Limestone do
  use Supervisor
  use Bedrock.Service.WorkerBehaviour

  alias Bedrock.Service.TransactionLogWorker.Limestone.{
    SegmentRecycler,
    Transactions,
    Server
  }

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

    children =
      [
        {SegmentRecycler,
         [
           minimum_available: minimum_available,
           segment_size: segment_size,
           path: path,
           otp_name: recycler_name
         ]},
        {Server,
         [
           id: id,
           otp_name: otp_name,
           controller: controller,
           transactions: transactions
         ]}
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
