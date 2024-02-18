defmodule Bedrock.Cluster.ServiceAdvertiser do
  use GenServer

  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.Coordinator

  defstruct ~w[cluster services controller]a

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

    PubSub.subscribe(cluster, :controller_changed)

    cluster.controller()
    |> case do
      {:ok, controller} ->
        {:ok, %{t | controller: controller}, {:continue, :advertise_services}}

      {:error, :unavailable} ->
        {:ok, t}
    end
  end

  @impl GenServer
  def handle_continue(:advertise_services, t) do
    Coordinator.join_cluster(t.controller, Node.self(), t.services)
    {:noreply, t}
  end

  @impl GenServer
  def handle_info({:controller_changed, new_controller}, t) when t.controller != new_controller,
    do: {:noreply, %{t | controller: new_controller}, {:continue, :advertise_services}}

  def handle_info({:controller_changed, _}, t), do: {:noreply, t}
end
