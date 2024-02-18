defmodule Bedrock.Cluster.ServiceAdvertiser do
  @moduledoc """
  Advertises services to the cluster controller. This module is responsible for
  advertising the services that are available on the local node to the cluster
  controller. This is done by subscribing to the `:cluster_controller_replaced`
  topic and then advertising the services to the controller when a new one
  is started.
  """
  use GenServer

  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.ClusterController

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
      services: services,
      controller: :unavailable
    }

    PubSub.subscribe(cluster, :cluster_controller_replaced)

    cluster.controller()
    |> case do
      {:ok, controller} ->
        {:ok, %{t | controller: controller}, {:continue, :advertise_services}}

      {:error, :unavailable} ->
        {:ok, t}
    end
  end

  @impl GenServer
  def handle_continue(:advertise_services, %{controller: :unavailable} = t),
    do: {:noreply, t}

  def handle_continue(:advertise_services, t) do
    t.controller
    |> ClusterController.join_cluster(Node.self(), t.services)
    |> case do
      :ok ->
        IO.inspect({:advertise_services, t})
        {:noreply, t}

      {:error, :unavailable} ->
        {:noreply, %{t | controller: :unavailable}}
    end

    {:noreply, t}
  end

  @impl GenServer
  def handle_info({:cluster_controller_replaced, new_controller}, t)
      when t.controller == new_controller,
      do: {:noreply, t}

  def handle_info({:cluster_controller_replaced, new_controller}, t),
    do: {:noreply, %{t | controller: new_controller}, {:continue, :advertise_services}}
end
