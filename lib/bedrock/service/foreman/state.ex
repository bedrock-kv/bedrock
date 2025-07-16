defmodule Bedrock.Service.Foreman.State do
  alias Bedrock.Cluster
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Worker

  @type t :: %__MODULE__{
          cluster: Cluster.t(),
          capabilities: [Cluster.capability()],
          health: :starting | :ok | :error,
          otp_name: atom(),
          path: Path.t(),
          waiting_for_healthy: [pid()],
          workers: %{Worker.id() => WorkerInfo.t()}
        }
  defstruct [
    :cluster,
    :capabilities,
    :default_worker,
    :health,
    :otp_name,
    :path,
    :waiting_for_healthy,
    :workers
  ]

  def new_state(%{
        cluster: cluster,
        capabilities: capabilities,
        path: path,
        otp_name: otp_name
      }) do
    {:ok,
     %__MODULE__{
       cluster: cluster,
       capabilities: capabilities,
       path: path,
       otp_name: otp_name,
       #
       health: :starting,
       waiting_for_healthy: [],
       workers: %{}
     }}
  end

  def new_state(_), do: {:error, :missing_required_params}

  def update_workers(t, updater), do: %{t | workers: updater.(t.workers)}

  def update_health(t, updater), do: %{t | health: updater.(t.health)}

  def put_health(t, health), do: %{t | health: health}

  @spec update_waiting_for_healthy(State.t(), ([GenServer.from()] -> [GenServer.from()])) ::
          State.t()
  def update_waiting_for_healthy(t, updater),
    do: %{t | waiting_for_healthy: updater.(t.waiting_for_healthy)}

  @spec put_waiting_for_healthy(State.t(), [pid()]) :: State.t()
  def put_waiting_for_healthy(t, waiting_for_healthy),
    do: %{t | waiting_for_healthy: waiting_for_healthy}

  @spec put_health_for_worker(State.t(), Worker.id(), Worker.health()) :: State.t()
  def put_health_for_worker(t, worker_id, health),
    do:
      update_workers(t, fn workers ->
        workers
        |> Map.update!(worker_id, &WorkerInfo.put_health(&1, health))
      end)
end
