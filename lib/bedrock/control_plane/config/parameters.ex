defmodule Bedrock.ControlPlane.Config.Parameters do
  @moduledoc """
  A `Parameters` is a data structure that describes the t that are used
  to configure the cluster.
  """

  @typedoc """
  Struct representing the parameters that are used to configure the cluster.

  ## Fields:
  - `nodes` - A list of nodes that are participating in the cluster.
  - `ping_rate_in_hz` - The rate at which the director is to ping the nodes,
    expressed in Hertz.
  - `retransmission_rate_in_hz` - The rate at which the system is to retransmit
    messages, expressed in Hertz.
  - `replication_factor` - The (minimum) number of nodes that must acknowledge a
    write before it is considered successful.
  - `desired_coordinators` - The number of coordinators that are to be made
    available within the system.
  - `desired_logs` - The number of transaction logs that are to be made
    available
  - `desired_read_version_proxies` - The number of get read version proxies
    that are to be made available as part of the transaction system.
  - `desired_commit_proxies` - The number of commit proxies that are to be made
    available as part of the transaction system.
  - `desired_transaction_resolvers` - The number of transaction resolvers that
    are to be made available as part of the transaction system.
  """

  @type rate_in_hz :: pos_integer()
  @type replication_factor :: pos_integer()

  @type t :: %__MODULE__{
          nodes: [node()],
          ping_rate_in_hz: rate_in_hz(),
          retransmission_rate_in_hz: rate_in_hz(),
          desired_replication_factor: replication_factor(),
          desired_coordinators: pos_integer(),
          desired_logs: pos_integer(),
          desired_read_version_proxies: pos_integer(),
          desired_commit_proxies: pos_integer(),
          desired_resolvers: pos_integer(),
          transaction_window_in_ms: pos_integer()
        }
  defstruct nodes: [],
            ping_rate_in_hz: 10,
            retransmission_rate_in_hz: 20,
            desired_replication_factor: 1,
            desired_coordinators: 1,
            desired_logs: 1,
            desired_read_version_proxies: 1,
            desired_commit_proxies: 1,
            desired_resolvers: 1,
            transaction_window_in_ms: 5_000

  @spec new(coordinators :: [node()]) :: t()
  def new(coordinators),
    do: %__MODULE__{
      nodes: coordinators,
      desired_coordinators: length(coordinators)
    }

  @spec put_desired_replication_factor(t(), replication_factor()) :: t()
  def put_desired_replication_factor(t, replication_factor),
    do: %{t | desired_replication_factor: replication_factor}
end
