defmodule Bedrock.DataPlane.Log.Limestone do
  alias Bedrock.DataPlane.Log.Limestone.Transactions

  use Supervisor
  use Bedrock.Service.WorkerBehaviour, kind: :log

  @doc false
  @spec child_spec(opts :: keyword() | []) :: Supervisor.child_spec()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id)
    path = Keyword.fetch!(opts, :path)
    foreman = Keyword.fetch!(opts, :foreman)

    %{
      id: {__MODULE__, id},
      start:
        {Supervisor, :start_link,
         [
           __MODULE__,
           {
             otp_name,
             id,
             foreman,
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
        {otp_name, id, foreman, _path, _min_spare_segments, _max_spare_segments, _segment_size}
      ) do
    transactions = Transactions.new(:"#{otp_name}_transactions")

    children =
      [
        {__MODULE__.Server,
         [
           id: id,
           otp_name: otp_name,
           foreman: foreman,
           transactions: transactions
         ]}
      ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
