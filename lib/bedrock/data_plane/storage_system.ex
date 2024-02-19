defmodule Bedrock.DataPlane.StorageSystem do
  use Supervisor
  use Bedrock.Cluster, :types

  def child_spec(opts) do
    cluster = Keyword.get(opts, :cluster) || raise "Missing :cluster option"

    path =
      Keyword.get(opts, :path) ||
        raise "Missing :path option; required when :storage is specified in :services"

    default_engine =
      Keyword.get(opts, :default_engine) ||
        Bedrock.DataPlane.StorageSystem.Engine.Basalt

    otp_name = cluster.otp_name(:storage_system)

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
    engine_supervisor_otp_name = otp_name(otp_name, :engine_supervisor)
    controller_otp_name = otp_name(otp_name, :controller)

    children = [
      {DynamicSupervisor, name: engine_supervisor_otp_name},
      {Bedrock.Worker.Controller,
       [
         cluster: cluster,
         subsystem: :storage_system,
         default_engine: default_engine,
         engine_supervisor_otp_name: engine_supervisor_otp_name,
         path: path,
         otp_scope: otp_name,
         otp_name: controller_otp_name
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp otp_name(otp_name, service), do: :"#{otp_name}_#{service}"

  def engines(t) when is_binary(t),
    do: otp_name(t, :controller) |> engines()

  defdelegate engines(t), to: Bedrock.Worker.Controller
end
