defmodule Bedrock.DataPlane.LogSystem do
  use Supervisor
  use Bedrock.Cluster, :types

  @type id :: binary()

  @doc """
  Starts a supervisor for the log system.
  """
  @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = Keyword.get(opts, :cluster) || raise "Missing :cluster option"

    path =
      Keyword.get(opts, :path) ||
        raise "Missing :path option; required when :log_system is specified in :services"

    default_engine =
      Keyword.get(opts, :default_engine) ||
        Bedrock.DataPlane.LogSystem.Engine.Limestone

    otp_name = cluster.otp_name(:log_system)

    %{
      id: __MODULE__,
      start: {
        Supervisor,
        :start_link,
        [
          __MODULE__,
          {cluster, path, default_engine, otp_name},
          [name: otp_name]
        ]
      },
      restart: :permanent
    }
  end

  @impl Supervisor
  def init({cluster, path, default_engine, otp_name}) do
    engine_supervisor_otp_name = cluster.otp_name(:log_system_engine_supervisor)
    controller_otp_name = cluster.otp_name(:log_system_controller)

    children = [
      {DynamicSupervisor, name: engine_supervisor_otp_name},
      {Bedrock.Engine.Controller,
       [
         cluster: cluster,
         default_engine: default_engine,
         engine_supervisor_otp_name: engine_supervisor_otp_name,
         path: path,
         otp_scope: otp_name,
         otp_name: controller_otp_name
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec engines(cluster :: Module.t()) :: {:ok, [Bedrock.Engine.t()]} | {:error, term()}
  def engines(t),
    do: t.otp_name(:log_system_controller) |> Bedrock.Engine.Controller.engines()

  @spec wait_for_healthy(cluster :: Module.t(), :infinity | non_neg_integer()) ::
          :ok | {:error, any()}
  def wait_for_healthy(cluster, timeout) do
    cluster.otp_name(:log_system_controller)
    |> Bedrock.Engine.Controller.wait_for_healthy(timeout)
  end
end
