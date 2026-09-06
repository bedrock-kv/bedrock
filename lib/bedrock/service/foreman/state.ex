defmodule Bedrock.Service.Foreman.State do
  @moduledoc false
  alias Bedrock.Cluster
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Worker

  @type t :: %__MODULE__{
          cluster: Cluster.t(),
          capabilities: [Cluster.capability()],
          health: Foreman.health(),
          otp_name: atom(),
          path: Path.t(),
          object_storage: term(),
          recovery_authority_migration: :disabled | :allow_legacy,
          workers: %{Worker.id() => WorkerInfo.t()}
        }
  defstruct [
    :cluster,
    :capabilities,
    :default_worker,
    :health,
    :otp_name,
    :path,
    :object_storage,
    :recovery_authority_migration,
    :workers
  ]

  @spec new_state(map()) :: {:ok, State.t()} | {:error, :missing_required_params}
  def new_state(%{cluster: c, capabilities: caps, path: path, otp_name: name, object_storage: storage} = args) do
    {:ok,
     %__MODULE__{
       cluster: c,
       capabilities: caps,
       path: path,
       otp_name: name,
       object_storage: storage,
       recovery_authority_migration: Map.get(args, :recovery_authority_migration, :disabled),
       #
       health: :starting,
       workers: %{}
     }}
  end

  def new_state(_), do: {:error, :missing_required_params}

  @spec update_workers(
          State.t(),
          (%{Worker.id() => WorkerInfo.t()} -> %{Worker.id() => WorkerInfo.t()})
        ) :: State.t()
  def update_workers(t, updater), do: %{t | workers: updater.(t.workers)}

  @spec update_health(State.t(), (Foreman.health() -> Foreman.health())) :: State.t()
  def update_health(t, updater), do: %{t | health: updater.(t.health)}

  @spec put_health(State.t(), Foreman.health()) :: State.t()
  def put_health(t, health), do: %{t | health: health}

  @spec put_health_for_worker(State.t(), Worker.id(), Worker.health()) :: State.t()
  def put_health_for_worker(t, worker_id, health),
    do: update_workers(t, fn workers -> Map.update!(workers, worker_id, &WorkerInfo.put_health(&1, health)) end)
end
