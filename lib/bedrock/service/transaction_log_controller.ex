defmodule Bedrock.Service.TransactionLogController do
  @moduledoc """
  """

  alias Bedrock.Service.Worker

  @type t :: GenServer.server()
  @type id :: binary()

  @doc """
  """
  @spec workers(t()) :: {:ok, [Worker.worker()]} | {:error, term()}
  defdelegate workers(t), to: Bedrock.Service.Controller

  @spec wait_for_healthy(t(), :infinity | non_neg_integer()) :: :ok | {:error, any()}
  def wait_for_healthy(cluster, timeout) do
    cluster.otp_name(:transaction_log)
    |> Bedrock.Service.Controller.wait_for_healthy(timeout)
  end

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Impl

  defmodule Impl do
    use Supervisor

    @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
    def child_spec(opts) do
      cluster = Keyword.get(opts, :cluster) || raise "Missing :cluster option"

      path =
        Keyword.get(opts, :path) ||
          raise "Missing :path option; required when :transaction_log is specified in :services"

      default_worker =
        Keyword.get(opts, :default_worker) ||
          Bedrock.Service.TransactionLog.Limestone

      otp_name = cluster.otp_name(:transaction_log)

      %{
        id: __MODULE__,
        start: {
          Supervisor,
          :start_link,
          [
            __MODULE__,
            {cluster, path, default_worker, otp_name}
          ]
        },
        restart: :permanent
      }
    end

    @impl Supervisor
    def init({cluster, path, default_worker, otp_name}) do
      worker_supervisor_otp_name = cluster.otp_name(:transaction_log_worker_supervisor)

      children = [
        {DynamicSupervisor, name: worker_supervisor_otp_name},
        {Bedrock.Service.Controller,
         [
           cluster: cluster,
           subsystem: :transaction_log,
           default_worker: default_worker,
           worker_supervisor_otp_name: worker_supervisor_otp_name,
           path: path,
           otp_name: otp_name
         ]}
      ]

      Supervisor.init(children, strategy: :one_for_one)
    end
  end
end
