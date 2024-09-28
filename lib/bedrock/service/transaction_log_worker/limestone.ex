defmodule Bedrock.Service.TransactionLogWorker.Limestone do
  use Supervisor
  use Bedrock.Service.WorkerBehaviour

  alias Bedrock.Service.TransactionLogWorker.Limestone.{
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
             controller,
             path,
             opts[:min_available] || 3,
             opts[:max_available] || 5,
             opts[:segment_size] || 64 * 1024 * 1024
           }
         ]},
      type: :supervisor
    }
  end

  @impl Supervisor
  def init({otp_name, id, controller, _path, _min_available, _max_available, _segment_size}) do
    transactions = Transactions.new(:"#{otp_name}_transactions")

    sup_name = :"#{otp_name}_sup"
    recycler_name = :"#{otp_name}_recycler"

    children =
      [
        {Server,
         id: id,
         otp_name: otp_name,
         controller: controller,
         transactions: transactions,
         recycler: recycler_name},
        {DynamicSupervisor, name: sup_name}
      ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
