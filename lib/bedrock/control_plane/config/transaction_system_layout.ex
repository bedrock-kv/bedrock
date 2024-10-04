defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout do
  @moduledoc """
  A `TransactionSystemLayout` is a data structure that describes the layout of
  the transaction system within the cluster.
  """

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionResolverDescriptor
  alias Bedrock.DataPlane.Log

  @typep log_id :: Log.id()
  @typep tag :: Bedrock.tag()

  @typedoc """
  Struct representing the layout of the transaction system within the cluster.

  ## Fields
    - `controller` - The full otp name of the cluster controller.
    - `sequencer` - The full otp name of the cluster sequencer.
    - `rate_keeper` - The full otp name of the system rate-keeper.
    - `data_distributor` - The full otp name of the cluster data distributor.
    - `read_version_proxies` - The full otp names of the read-version proxies.
    - `commit_proxies` - The full otp names of the commit proxies.
    - `transaction_resolvers` - The full otp names of the transaction resolvers.
    - `logs` - A list of logs that are responsible for storing the transactions on
       their way to the storage teams. Each log contains a list of the tags
       that it services, and the full otp name of the log worker process that
       is responsible for the log.
    - `storage_teams` - A list of storage teams that are responsible for storing the data within
       the system. Each team represents a shard of the key space, and contains
       a list of the storage worker ids that are responsible for the shard.
    - `service_directory` - A list of all of the workers within the system, their types, ids and
       the otp names used to communicate with them.
  """
  @type t :: %__MODULE__{
          controller: pid(),
          sequencer: pid(),
          rate_keeper: pid(),
          data_distributor: pid(),
          proxies: [pid()],
          transaction_resolvers: [TransactionResolverDescriptor.t()],
          logs: [LogDescriptor.t()],
          storage_teams: [StorageTeamDescriptor.t()],
          service_directory: [ServiceDescriptor.t()]
        }

  defstruct controller: nil,
            sequencer: nil,
            rate_keeper: nil,
            data_distributor: nil,
            proxies: [],
            transaction_resolvers: [],
            logs: [],
            storage_teams: [],
            service_directory: []

  @spec set_controller(t(), pid()) :: t()
  def set_controller(t, controller), do: put_in(t.controller, controller)

  @doc """
  Inserts a log descriptor into the transaction system layout, replacing any
  existing log descriptor with the same id.
  """
  @spec insert_log(t(), LogDescriptor.t()) :: t()
  def insert_log(t, %LogDescriptor{} = descriptor),
    do: update_in(t.logs, &LogDescriptor.upsert(&1, descriptor))

  @doc """
  Get a log descriptor by its id or nil if not found.
  """
  @spec find_log_by_id(t(), log_id()) :: LogDescriptor.t() | nil
  def find_log_by_id(t, id),
    do: get_in(t.logs) |> LogDescriptor.find_by_id(id)

  @doc """
  Removes a log descriptor by its id.
  """
  @spec remove_log_with_id(t(), log_id()) :: t()
  def remove_log_with_id(t, id),
    do: update_in(t.logs, &LogDescriptor.remove_by_id(&1, id))

  @doc """
  Inserts a storage team descriptor into the transaction system layout,
  replacing any existing storage team descriptor with the same tag.
  """
  @spec insert_storage_team(t(), StorageTeamDescriptor.t()) :: t()
  def insert_storage_team(t, %StorageTeamDescriptor{} = descriptor),
    do: update_in(t.storage_teams, &StorageTeamDescriptor.upsert(&1, descriptor))

  @doc """
  Get a storage team descriptor by its tag or nil if not found.
  """
  @spec find_storage_team_by_tag(t(), tag()) :: LogDescriptor.t() | nil
  def find_storage_team_by_tag(t, tag),
    do: get_in(t.storage_teams) |> StorageTeamDescriptor.find_by_tag(tag)

  @doc """
  Removes a log descriptor by its id.
  """
  @spec remove_storage_team_with_tag(t(), tag()) :: t()
  def remove_storage_team_with_tag(t, tag),
    do: update_in(t.storage_teams, &StorageTeamDescriptor.remove_by_tag(&1, tag))
end
