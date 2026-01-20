defmodule Bedrock.DataPlane.CommitProxy do
  @moduledoc """
  Central coordinator of Bedrock's transaction commit process.

  The Commit Proxy batches transactions from multiple clients for efficient processing,
  orchestrates conflict resolution through Resolvers, and ensures durable persistence
  across all required log servers. It transforms individual transaction requests into
  efficiently processed batches while maintaining strict consistency guarantees.

  Transaction batching creates a fundamental trade-off between latency and throughput.
  The Commit Proxy manages this through configurable size and time limits that balance
  responsiveness against processing efficiency. This batching strategy enables
  intra-batch conflict detection and amortizes the fixed costs of conflict resolution
  and logging across multiple transactions while preserving the arrival order of
  transactions within each batch.

  The component uses a fail-fast recovery model where unrecoverable errors trigger
  process exit and Director-coordinated recovery. Commit Proxies start in locked mode
  and require explicit unlocking through `recover_from/3` before accepting transaction
  commits, ensuring proper coordination during cluster recovery scenarios.

  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.Transaction

  @type ref :: pid() | atom() | {atom(), node()}

  @doc """
  Unlocks a commit proxy and provides the transaction system layout.

  Called by the Director during recovery to transition the commit proxy from
  `:locked` to `:running` mode with full routing information including shard
  layout and log mappings needed to route transactions.
  """
  @spec recover_from(
          commit_proxy_ref :: ref(),
          lock_token :: binary(),
          sequencer :: pid(),
          resolver_layout :: ResolverLayout.t(),
          routing_data :: RoutingData.t()
        ) :: :ok | {:error, :timeout} | {:error, :unavailable}
  def recover_from(commit_proxy, lock_token, sequencer, resolver_layout, routing_data),
    do: call(commit_proxy, {:recover_from, lock_token, sequencer, resolver_layout, routing_data}, :infinity)

  @spec commit(commit_proxy_ref :: ref(), epoch :: Bedrock.epoch(), transaction :: Transaction.encoded()) ::
          {:ok, version :: Bedrock.version(), index :: non_neg_integer()}
          | {:error, :wrong_epoch | :timeout | :unavailable}
  def commit(commit_proxy, epoch, transaction), do: call(commit_proxy, {:commit, epoch, transaction}, :infinity)
end
