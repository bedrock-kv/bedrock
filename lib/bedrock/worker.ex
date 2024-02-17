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

  alias __MODULE__.Manager

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
    manager_otp_name = cluster.otp_name(:worker_manager)

    children =
      [
        {DynamicSupervisor, name: cluster.otp_name(:sup)},
        {Manager,
         [
           cluster: cluster,
           services: services,
           otp_name: manager_otp_name
         ]}
        | services
          |> Enum.reduce([], fn
            service, acc ->
              [
                {module_for_service(service),
                 [
                   {:cluster, cluster},
                   {:manager, manager_otp_name} | config[service]
                 ]}
                | acc
              ]
          end)
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp module_for_service(:coordinator), do: Bedrock.ControlPlane.Coordinator
  defp module_for_service(:storage), do: Bedrock.DataPlane.StorageSystem
  defp module_for_service(:log_system), do: Bedrock.DataPlane.LogSystem

  defmodule Manager do
    use GenServer

    alias Bedrock.ControlPlane.Coordinator

    defstruct ~w[cluster services coordinator]a

    def child_spec(opts) do
      cluster = opts[:cluster] || raise "Missing :cluster option"
      services = opts[:services] || raise "Missing :services option"
      otp_name = opts[:otp_name] || raise "Missing :otp_name option"

      %{
        id: __MODULE__,
        start: {
          GenServer,
          :start_link,
          [
            __MODULE__,
            {cluster, services},
            [name: otp_name]
          ]
        },
        restart: :permanent
      }
    end

    @impl GenServer
    def init({cluster, services}) do
      t = %__MODULE__{
        cluster: cluster,
        services: services
      }

      {:ok, t, {:continue, :find_a_live_coordinator}}
    end

    @impl GenServer
    def handle_continue(:find_a_live_coordinator, t) do
      t.cluster.coordinator()
      |> case do
        {:ok, coordinator} ->
          {:noreply, %{t | coordinator: coordinator}, {:continue, :attempt_to_join_cluster}}

        {:error, :unavailable} ->
          Process.send_after(self(), :find_a_live_coordinator, 1_000)
          {:noreply, t}
      end

      {:noreply, t}
    end

    def handle_continue(:attempt_to_join_cluster, t) do
      t.coordinator
      |> Coordinator.join_cluster({t.otp_name, Node.self()}, t.services)
      |> case do
        :ok ->
          {:noreply, t}

        {:error, :unavailable} ->
          {:noreply, %{t | coordinator: nil}, {:continue, :find_a_live_coordinator}}
      end

      {:noreply, t}
    end

    @impl GenServer
    def handle_info(:find_a_live_coordinator, t),
      do: {:noreply, t, {:continue, :find_a_live_coordinator}}
  end
end
