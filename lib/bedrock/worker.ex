defmodule Bedrock.Worker do
  @moduledoc """
  A worker is a process that runs on a node and offers a set of services to the
  cluster. It is a supervisor that starts and manages the lifecycle of the
  services it offers.
  """

  @default_services ~w[
    coordinator
    log_system
    storage
  ]a

  def default_services, do: @default_services

  use Supervisor
  require Logger

  use Bedrock.Cluster, :types

  defmacro __using__(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"

    services = Keyword.get(opts, :services)

    static_config =
      opts
      |> Keyword.take([:coordinator, :storage, :log_system])
      |> Keyword.put_new(:coordinator, [])
      |> Keyword.put_new(:storage, [])
      |> Keyword.put_new(:log_system, [])

    quote do
      alias Bedrock.Client
      alias Bedrock.Worker

      def child_spec(opts) do
        config =
          merge_configs(
            unquote(static_config),
            unquote(cluster).config(),
            opts
          )

        services =
          [
            unquote(services),
            config |> Keyword.get(:services),
            opts[:services]
          ]
          |> Enum.find(Worker.default_services(), &(&1 not in [[], nil]))

        %{
          id: __MODULE__,
          start:
            {Supervisor, :start_link,
             [
               Bedrock.Worker,
               {unquote(cluster), services, config},
               [name: unquote(cluster).otp_name(:worker)]
             ]},
          restart: :permanent,
          type: :supervisor
        }
      end

      defp merge_configs(static, runtime, args) do
        static
        |> Enum.reduce(runtime, fn
          {service, service_config}, acc ->
            Keyword.put(
              acc,
              service,
              service_config
              |> Keyword.merge(runtime |> Keyword.get(service, []))
              |> Keyword.merge(args |> Keyword.get(service, []))
            )
        end)
      end
    end
  end

  def init({cluster, services, config}) do
    Logger.debug(
      "Worker #{Node.self()} up, offering: #{services |> Enum.map_join(", ", &Atom.to_string/1)}",
      cluster: cluster
    )

    children =
      [
        {DynamicSupervisor, name: cluster.otp_name(:sup)}
        | services
          |> Enum.reduce([], fn
            service, acc ->
              [
                {module_for_service(service), [{:cluster, cluster} | config[service]]}
                | acc
              ]
          end)
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp module_for_service(:coordinator), do: Bedrock.ControlPlane.Coordinator
  defp module_for_service(:storage), do: Bedrock.DataPlane.StorageSystem
  defp module_for_service(:log_system), do: Bedrock.DataPlane.LogSystem
end
