defmodule Bedrock.Cluster.Monitor.Advertising do
  alias Bedrock.Cluster.Monitor.State
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.Service.Controller
  alias Bedrock.Service.Worker
  alias Bedrock.Cluster.PubSub

  require Logger

  @spec advertise_capabilities(State.t()) ::
          {:ok, State.t()} | {:error, :unavailable | :nodes_must_be_added_by_an_administrator}
  def advertise_capabilities(t) do
    t.controller
    |> ClusterController.request_to_rejoin(
      Node.self(),
      t.capabilities,
      running_services(t)
    )
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

  @spec running_services(State.t()) :: [keyword()]
  def running_services(t) do
    t.capabilities
    |> Enum.flat_map(fn
      service ->
        service
        |> t.cluster.otp_name()
        |> Controller.all()
        |> case do
          {:ok, worker_pids} ->
            worker_pids |> gather_info_from_workers()

          {:error, :unavailable} ->
            []

          {:error, reason} ->
            Logger.error("Failed to get workers for #{service}: #{inspect(reason)}")
            []
        end
    end)
  end

  @spec gather_info_from_workers([pid()]) :: [keyword()]
  def gather_info_from_workers(worker_pids) do
    worker_pids
    |> Enum.reduce([], fn worker_pid, list ->
      gather_info_from_worker(worker_pid)
      |> case do
        {:ok, info} -> [info | list]
        _ -> list
      end
    end)
  end

  @spec gather_info_from_worker(Worker.ref()) :: {:ok, map()} | {:error, :unavailable}
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
