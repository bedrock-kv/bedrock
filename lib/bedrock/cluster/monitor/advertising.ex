defmodule Bedrock.Cluster.Monitor.Advertising do
  alias Bedrock.Cluster.Monitor.State
  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  import Bedrock.Cluster.Monitor.Telemetry,
    only: [
      trace_advertising_capabilities: 3
    ]

  @spec advertise_capabilities(State.t()) ::
          {:ok, State.t()}
          | {:error, :unavailable}
          | {:error, :nodes_must_be_added_by_an_administrator}
          | {:error, {:relieved_by, {Bedrock.epoch(), pid()}}}
  def advertise_capabilities(t) do
    running_services = running_services(t)

    trace_advertising_capabilities(t.cluster, t.capabilities, running_services)

    t.controller
    |> ClusterController.request_to_rejoin(Node.self(), t.capabilities, running_services)
    |> case do
      :ok -> {:ok, t}
      {:error, _reason} = error -> error
    end
  end

  @spec advertise_worker_to_cluster_controller(State.t(), Worker.ref()) :: State.t()
  def advertise_worker_to_cluster_controller(t, worker_pid) do
    with {:ok, info} <- gather_info_from_worker(worker_pid),
         :ok <- t.controller |> ClusterController.advertise_worker(node(), info) do
      t
    else
      _ -> t
    end
  end

  @spec running_services(State.t()) :: ClusterController.running_service_info_by_id()
  def running_services(t) do
    case Foreman.all(t.cluster.otp_name(:foreman)) do
      {:ok, worker_pids} -> worker_pids |> gather_info_from_workers()
      {:error, _reason} -> %{}
    end
  end

  @spec gather_info_from_workers([pid()]) :: ClusterController.running_service_info_by_id()
  def gather_info_from_workers(worker_pids) do
    worker_pids
    |> Enum.map(&gather_info_from_worker/1)
    |> Enum.filter(fn
      {:ok, _} -> true
      _ -> false
    end)
    |> Map.new(fn {:ok, info} -> {info[:id], info} end)
  end

  @spec gather_info_from_worker(Worker.ref()) ::
          {:ok, ClusterController.running_service_info()}
          | {:error, :unavailable}
  def gather_info_from_worker(worker),
    do: Worker.info(worker, [:id, :otp_name, :kind, :pid])

  @spec publish_cluster_controller_replaced_to_pubsub(State.t()) :: State.t()
  def publish_cluster_controller_replaced_to_pubsub(t) do
    PubSub.publish(
      t.cluster,
      :cluster_controller_replaced,
      {:cluster_controller_replaced, t.controller}
    )

    t
  end
end
