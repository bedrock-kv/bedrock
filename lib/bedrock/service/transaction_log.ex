defmodule Bedrock.Service.TransactionLog do
  use Supervisor
  use Bedrock.Cluster, :types

  @type id :: binary()

  defmodule Config do
    defstruct [:write_quorum, :replication_factor, :tlogs]
  end

  @doc """
  Starts a supervisor for the log system.
  """
  @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = Keyword.get(opts, :cluster) || raise "Missing :cluster option"

    path =
      Keyword.get(opts, :path) ||
        raise "Missing :path option; required when :log_system is specified in :services"

    default_worker =
      Keyword.get(opts, :default_worker) ||
        Bedrock.Service.TransactionLogWorker.Limestone

    otp_name = cluster.otp_name(:log_system)

    %{
      id: __MODULE__,
      start: {
        Supervisor,
        :start_link,
        [
          __MODULE__,
          {cluster, path, default_worker, otp_name},
          [name: otp_name]
        ]
      },
      restart: :permanent
    }
  end

  @impl Supervisor
  def init({cluster, path, default_worker, otp_name}) do
    worker_supervisor_otp_name = cluster.otp_name(:log_system_worker_supervisor)
    controller_otp_name = cluster.otp_name(:log_system_controller)

    children = [
      {DynamicSupervisor, name: worker_supervisor_otp_name},
      {Bedrock.Service.Controller,
       [
         cluster: cluster,
         subsystem: :log_system,
         default_worker: default_worker,
         worker_supervisor_otp_name: worker_supervisor_otp_name,
         path: path,
         otp_scope: otp_name,
         otp_name: controller_otp_name
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec workers(cluster :: Module.t()) :: {:ok, [Bedrock.Service.t()]} | {:error, term()}
  def workers(t),
    do: t.otp_name(:log_system_controller) |> Bedrock.Service.Controller.workers()

  @spec wait_for_healthy(cluster :: Module.t(), :infinity | non_neg_integer()) ::
          :ok | {:error, any()}
  def wait_for_healthy(cluster, timeout) do
    cluster.otp_name(:log_system_controller)
    |> Bedrock.Service.Controller.wait_for_healthy(timeout)
  end
end
