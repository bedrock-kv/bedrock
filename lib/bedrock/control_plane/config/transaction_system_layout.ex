defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout do
  @moduledoc """
  A `TransactionSystemLayout` is a data structure that describes the layout of
  the transaction system within the cluster.
  """

  alias Bedrock.DataPlane.Log
  alias Bedrock.Service.Worker
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor

  @typedoc """
  Struct representing the layout of the transaction system within the cluster.

  ## Fields
    - `id` - The unique identifier of the layout.
    - `director` - The full otp name of the cluster director.
    - `sequencer` - The full otp name of the cluster sequencer.
    - `rate_keeper` - The full otp name of the system rate-keeper.
    - `read_version_proxies` - The full otp names of the read-version proxies.
    - `commit_proxies` - The full otp names of the commit proxies.
    - `resolvers` - The pids of the transaction resolvers.
    - `logs` - A list of logs that are responsible for storing the transactions on
       their way to the storage teams. Each log contains a list of the tags
       that it services, and the full otp name of the log worker process that
       is responsible for the log.
    - `storage_teams` - A list of storage teams that are responsible for storing the data within
       the system. Each team represents a shard of the key space, and contains
       a list of the storage worker ids that are responsible for the shard.
    - `services` - A list of all of the workers within the system, their types, ids and
       the otp names used to communicate with them.
  """
  @type process_ref :: pid() | nil
  @type proxy_list :: [pid()]
  @type resolver_list :: [ResolverDescriptor.t()]
  @type log_map :: %{Log.id() => LogDescriptor.t()}
  @type storage_team_list :: [StorageTeamDescriptor.t()]
  @type service_map :: %{Worker.id() => ServiceDescriptor.t()}

  @type t :: %{
          id: id(),
          epoch: non_neg_integer(),
          director: process_ref() | :unavailable,
          sequencer: process_ref(),
          rate_keeper: process_ref(),
          proxies: proxy_list(),
          resolvers: resolver_list(),
          logs: log_map(),
          storage_teams: storage_team_list(),
          services: service_map()
        }

  @type id :: non_neg_integer()

  @spec default() :: t()
  def default(),
    do: %{
      id: 0,
      epoch: 0,
      director: nil,
      sequencer: nil,
      rate_keeper: nil,
      proxies: [],
      resolvers: [],
      logs: %{},
      storage_teams: [],
      services: %{}
    }

  @spec random_id() :: id()
  def random_id, do: :rand.uniform(1_000_000)
end
