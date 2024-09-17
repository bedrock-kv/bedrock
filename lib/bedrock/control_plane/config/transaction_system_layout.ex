defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout do
  alias Bedrock.ControlPlane.Config.ServiceDescriptor

  defstruct [
    # The full otp name of the cluster controller.
    controller: nil,

    # The full otp name of the cluster sequencer.
    sequencer: nil,

    # The full otp name of the system rate-keeper.
    rate_keeper: nil,

    # The full otp name of the cluster data distributor.
    data_distributor: nil,

    # The full otp names of the get-read-version proxies.
    get_read_version_proxies: [],

    # The full otp names of the commit proxies.
    commit_proxies: [],

    # The full otp names of the transaction resolvers.
    transaction_resolvers: [],

    # A list of logs that are responsible for storing the transactions on
    # their way to the storage teams. Each log contains a list of the tags
    # that it services, and the full otp name of the log worker process that
    # is responsible for the log.
    logs: [],

    # A list of storage teams that are responsible for storing the data within
    # the system. Each team represents a shard of the key space, and contains
    # a list of the storage worker ids that are responsible for the shard.
    storage_teams: [],

    # A list of all of the workers within the system, their types, ids and
    # the otp names used to communicate with them.
    service_directory: []
  ]

  @type t :: %__MODULE__{
          controller: pid(),
          service_directory: [ServiceDescriptor.t()]
        }
end
