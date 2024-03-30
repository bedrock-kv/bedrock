defmodule Bedrock.ControlPlane.Config.Parameters do
  @type t :: %__MODULE__{}
  defstruct [
    # A list of nodes that are participating in the cluster.
    nodes: [],

    # The rate at which the controller is to ping the nodes, expressed in
    # Hertz
    ping_rate_in_hz: 10,

    # The rate at which the system is to retransmit messages, expressed in
    # Hertz.
    retransmission_rate_in_hz: 20,

    # The (minimum) number of nodes that must acknowledge a write before it is
    # considered successful.
    replication_factor: 1,

    # The number of coordinators that are to be made available within the
    # system.
    desired_coordinators: 1,

    # The number of transaction logs that are to be made available within the
    # system. Must be equal to or greater than the replication factor.
    desired_logs: 1,

    # The number of get read version proxies that are to be made available as
    # part of the transaction system.
    desired_get_read_version_proxies: 1,

    # The number of commit proxies that are to be made available as part of
    # the transaction system.
    desired_commit_proxies: 1,

    # The number of transaction resolvers that are to be made available as
    # part of the transaction system.
    desired_transaction_resolvers: 1
  ]
end
