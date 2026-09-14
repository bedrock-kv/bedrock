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
  - `materializer_idle_timeout_ms` - The read-inactivity window after which a
    data-shard materializer spins itself down. Zero disables spin-down.
  """

  @type rate_in_hz :: pos_integer()
  @type replication_factor :: pos_integer()

  @type t :: %{
          nodes: [node()],
          ping_rate_in_hz: rate_in_hz(),
          retransmission_rate_in_hz: rate_in_hz(),
          desired_replication_factor: replication_factor(),
          desired_coordinators: pos_integer(),
          desired_logs: pos_integer(),
          desired_read_version_proxies: pos_integer(),
          desired_commit_proxies: pos_integer(),
          transaction_window_in_ms: pos_integer(),
          materializer_idle_timeout_ms: non_neg_integer()
        }

  # Fifteen minutes. Only CLIENT reads count as activity — applying
  # transactions and pulling keep a shard fresh, not hot — so a
  # write-hot but read-cold shard spins down too, and the window has to
  # be long enough that no working set crosses it. The first read after
  # a spin-down pays a placeholder park plus a recruit and a snapshot
  # download, so the wrong answer here is cheap but not free; fifteen
  # minutes is well past any interactive gap and still releases a
  # genuinely cold shard's node within a maintenance window. Checks run
  # at a quarter of the window (~3m45s), so the timer costs nothing.
  @default_materializer_idle_timeout_ms 900_000

  @doc """
  The default read-inactivity window for a data-shard materializer:
  what a fresh cluster gets, and what a bootstrap record written before
  the parameter existed reads back as.
  """
  @spec default_materializer_idle_timeout_ms() :: pos_integer()
  def default_materializer_idle_timeout_ms, do: @default_materializer_idle_timeout_ms

  @spec new(coordinators :: [node()]) :: t()
  #
  def new(coordinators),
    do: %{
      nodes: coordinators,
      desired_coordinators: length(coordinators),
      ping_rate_in_hz: 10,
      retransmission_rate_in_hz: 20,
      desired_replication_factor: 1,
      desired_logs: 1,
      desired_read_version_proxies: 1,
      desired_commit_proxies: 1,
      transaction_window_in_ms: 5_000,
      materializer_idle_timeout_ms: @default_materializer_idle_timeout_ms
    }

  @spec put_desired_replication_factor(t(), replication_factor()) :: t()
  def put_desired_replication_factor(t, replication_factor), do: %{t | desired_replication_factor: replication_factor}
end
