defmodule Bedrock.DataPlane.Demux.Supervisor do
  @moduledoc """
  Supervisor for the Demux system.

  Supervises the Demux.Server (coordinator) and ShardServer.Supervisor (dynamic).
  Uses one_for_all strategy - if any child crashes, all are restarted.
  This ensures consistency since ShardServer crash means potential lost transactions.

  ## Supervision Tree

  ```
  Demux.Supervisor (one_for_all)
      │
      ├── Demux.Server (GenServer - coordinator)
      │
      └── ShardServer.Supervisor (DynamicSupervisor)
              │
              ├── ShardServer 0
              └── ShardServer N
  ```
  """

  use Supervisor

  alias Bedrock.DataPlane.Demux.Server
  alias Bedrock.DataPlane.Demux.ShardServerSupervisor

  @doc """
  Starts the Demux supervisor.

  ## Options

  - `:cluster` - Required. Cluster name for ObjectStorage paths.
  - `:object_storage` - Required. ObjectStorage backend.
  - `:log` - Required. PID of the owning Log for durability reporting.
  - `:name` - Optional. Name for the supervisor.
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    cluster = Keyword.fetch!(opts, :cluster)
    object_storage = Keyword.fetch!(opts, :object_storage)
    log = Keyword.fetch!(opts, :log)

    children = [
      # ShardServer supervisor must start first so Server can reference it
      {ShardServerSupervisor, cluster: cluster, object_storage: object_storage},
      {Server, cluster: cluster, object_storage: object_storage, log: log, shard_supervisor: ShardServerSupervisor}
    ]

    # one_for_all: if any child crashes, restart all
    # This ensures consistency - ShardServer crash = potential lost transactions
    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc """
  Returns the Demux.Server pid from the supervisor.
  """
  def server(supervisor) do
    children = Supervisor.which_children(supervisor)

    case Enum.find(children, fn {id, _, _, _} -> id == Server end) do
      {Server, pid, :worker, _} when is_pid(pid) -> {:ok, pid}
      _ -> {:error, :not_found}
    end
  end
end

defmodule Bedrock.DataPlane.Demux.ShardServerSupervisor do
  @moduledoc """
  DynamicSupervisor for ShardServers.

  ShardServers are started on-demand when a transaction first touches a shard.
  """

  use DynamicSupervisor

  alias Bedrock.DataPlane.Demux.ShardServer

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    # Store opts for later use when starting children
    Process.put(:shard_supervisor_opts, opts)
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts a ShardServer for the given shard.

  ## Options

  - `:shard_id` - Required. The shard ID.
  - `:demux` - Required. PID of the Demux.Server for durability reporting.
  - `:cluster` - Required. Cluster name.
  - `:object_storage` - Required. ObjectStorage backend.
  """
  def start_child(opts) do
    child_spec = {ShardServer, opts}
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end
end
