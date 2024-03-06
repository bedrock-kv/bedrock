defmodule Bedrock.Cluster.ServiceAdvertiser do
  @moduledoc """
  Advertises services to the cluster controller.

  This GenServer is responsible for advertising the services that are available
  on the local node to the cluster controller. This is done by subscribing to
  the `:cluster_controller_replaced` topic and then advertising the services to
  the controller when the controller is replaced.
  """
  use GenServer
  require Logger

  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.Service.Worker
  alias Bedrock.Service.Controller

  @type t :: %__MODULE__{
          cluster: Cluster.t(),
          advertised_services: [atom()],
          controller: ClusterController.t() | :unavailable
        }
  defstruct [:cluster, :advertised_services, :controller]

  defmodule Directory do
    @type t :: :ets.table()
    @type id :: String.t()
    @type class :: :log | :storage

    @spec new() :: t()
    def new(), do: :ets.new(:service_directory, [:ordered_set])

    @spec insert(t(), id(), class(), pid()) :: :ok
    def insert(t, id, service, pid) do
      :ets.insert(t, {id, service, pid})
      :ok
    end

    @spec remove(t(), String.t()) :: :ok
    def remove(t, id) do
      :ets.delete(t, id)
      :ok
    end

    @spec lookup(t(), String.t()) :: any() | nil
    def lookup(t, id), do: :ets.lookup(t, id) |> List.first()

    @spec export(t()) :: [tuple()]
    def export(t), do: :ets.tab2list(t)
  end

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    advertised_services = opts[:services] || raise "Missing :services option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"

    %{
      id: __MODULE__,
      start: {
        GenServer,
        :start_link,
        [
          __MODULE__,
          {cluster, advertised_services},
          [name: otp_name]
        ]
      },
      restart: :permanent
    }
  end

  @impl GenServer
  def init({cluster, advertised_services}) do
    t = %__MODULE__{
      cluster: cluster,
      advertised_services: advertised_services,
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
    |> ClusterController.request_to_rejoin(
      Node.self(),
      t.advertised_services,
      running_services(t)
    )
    |> case do
      :ok ->
        {:noreply, t}

      {:error, :unavailable} ->
        {:noreply, %{t | controller: :unavailable}}

      {:error, :nodes_must_be_added_by_an_administrator} ->
        Logger.error("This node must be added to the cluster by an administrator")
        {:noreply, t}
    end

    {:noreply, t}
  end

  @impl GenServer
  def handle_info({:cluster_controller_replaced, new_controller}, t)
      when t.controller == new_controller,
      do: {:noreply, t}

  def handle_info({:cluster_controller_replaced, new_controller}, t),
    do: {:noreply, %{t | controller: new_controller}, {:continue, :advertise_services}}

  @spec running_services(t()) :: [keyword()]
  def running_services(t) do
    t.advertised_services
    |> Enum.flat_map(fn
      service when service in [:transaction_log, :storage] ->
        t.cluster.otp_name(service)
        |> Controller.workers()
        |> case do
          {:ok, worker_pids} ->
            worker_pids |> gather_info_from_workers()

          {:error, :unavailable} ->
            []

          {:error, reason} ->
            Logger.error("Failed to get workers for #{service}: #{inspect(reason)}")
            []
        end

      :coordination ->
        []
    end)
  end

  @spec gather_info_from_workers([pid()]) :: [keyword()]
  def gather_info_from_workers(worker_pids) do
    worker_pids
    |> Enum.reduce([], fn worker_pid, list ->
      worker_pid
      |> Worker.info([:id, :otp_name, :kind, :pid])
      |> case do
        {:ok, info} -> [info | list]
        _ -> list
      end
    end)
  end
end
