defmodule Bedrock.DataPlane.Log.Limestone do
  alias Bedrock.DataPlane.Log.Limestone.Transactions

  use Supervisor
  use Bedrock.Service.WorkerBehaviour

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

  @impl true
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
end
