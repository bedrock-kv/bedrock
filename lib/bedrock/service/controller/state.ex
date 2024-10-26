defmodule Bedrock.Service.Controller.State do
  alias Bedrock.Service.Controller.State
  alias Bedrock.Service.Controller.WorkerInfo
  alias Bedrock.Service.Worker

  @type t :: %__MODULE__{
          cluster: module(),
          default_worker: module(),
          health: :starting | :ok | :error,
          otp_name: atom(),
          path: Path.t(),
          registry: module(),
          subsystem: atom(),
          waiting_for_healthy: [pid()],
          worker_supervisor_otp_name: atom(),
          workers: %{Worker.id() => WorkerInfo.t()}
        }
  defstruct [
    :cluster,
    :default_worker,
    :health,
    :otp_name,
    :path,
    :registry,
    :subsystem,
    :waiting_for_healthy,
    :worker_supervisor_otp_name,
    :workers
  ]

  def new_state(%{
        subsystem: subsystem,
        cluster: cluster,
        path: path,
        default_worker: default_worker,
        worker_supervisor_otp_name: worker_supervisor_otp_name,
        otp_name: otp_name
      }) do
    {:ok,
     %__MODULE__{
       subsystem: subsystem,
       cluster: cluster,
       path: path,
       default_worker: default_worker,
       worker_supervisor_otp_name: worker_supervisor_otp_name,
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

  def update_waiting_for_healthy(t, updater),
    do: %{t | waiting_for_healthy: updater.(t.waiting_for_healthy)}

  def put_waiting_for_healthy(t, waiting_for_healthy),
    do: %{t | waiting_for_healthy: waiting_for_healthy}

  @spec put_health_for_worker(State.t(), Worker.id(), Worker.health()) :: State.t()
  def put_health_for_worker(t, worker_id, health),
    do: put_in(t, [:workers, worker_id, :health], health)
end
