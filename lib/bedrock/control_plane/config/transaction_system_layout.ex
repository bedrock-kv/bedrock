defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout do
  @moduledoc """
  A `TransactionSystemLayout` is a data structure that describes the layout of
  the transaction system within the cluster.
  """

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionResolverDescriptor

  @typedoc """
  Struct representing the layout of the transaction system within the cluster.

  ## Fields
    - `id` - The unique identifier of the layout.
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
    - `services` - A list of all of the workers within the system, their types, ids and
       the otp names used to communicate with them.
  """
  @type t :: %__MODULE__{
          id: id(),
          controller: pid() | nil,
          sequencer: pid() | nil,
          rate_keeper: pid() | nil,
          data_distributor: pid() | nil,
          proxies: [pid()],
          transaction_resolvers: [TransactionResolverDescriptor.t()],
          logs: [LogDescriptor.t()],
          storage_teams: [StorageTeamDescriptor.t()],
          services: [ServiceDescriptor.t()]
        }

  @type id :: non_neg_integer()

  defstruct id: 0,
            controller: nil,
            sequencer: nil,
            rate_keeper: nil,
            data_distributor: nil,
            proxies: [],
            transaction_resolvers: [],
            logs: [],
            storage_teams: [],
            services: []

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec put_logs(t(), [LogDescriptor.t()]) :: t()
  def put_logs(t, logs), do: %{t | logs: logs}

  @spec put_storage_teams(t(), [StorageTeamDescriptor.t()]) :: t()
  def put_storage_teams(t, storage_teams), do: %{t | storage_teams: storage_teams}
end
