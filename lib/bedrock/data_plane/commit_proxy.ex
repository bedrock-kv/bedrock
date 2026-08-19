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
  and require explicit unlocking through `recover_from/5` before accepting transaction
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
          routing_snapshot :: RoutingData.snapshot()
        ) :: :ok | {:error, :timeout} | {:error, :unavailable}
  def recover_from(commit_proxy, lock_token, sequencer, resolver_layout, routing_snapshot),
    do: call(commit_proxy, {:recover_from, lock_token, sequencer, resolver_layout, routing_snapshot}, :infinity)

  @doc """
  Submits a transaction for commit.

  By default the commit is bounded to the user keyspace: any mutation keyed
  at or above `Bedrock.end_of_user_keyspace()` is rejected at ingress.
  Passing `mode: :system` extends the legal range to
  `Bedrock.end_of_keyspace()`, admitting writes to `\\xFF` system keys. The
  mode is asserted by the caller — like FoundationDB's `ACCESS_SYSTEM_KEYS`
  option it guards against accidental system writes, not hostile ones. Only
  system components (recovery's persistence phase, and eventually the
  Distributor) commit in system mode.

  Atomic operations on system keys are rejected in every mode: the metadata
  pipeline replays only sets and clears, so an atomic would let durable
  state diverge from every metadata view built from the commit stream.
  """
  @spec commit(
          commit_proxy_ref :: ref(),
          epoch :: Bedrock.epoch(),
          transaction :: Transaction.encoded(),
          opts :: [mode: :user | :system]
        ) ::
          {:ok, version :: Bedrock.version(), index :: non_neg_integer()}
          | {:error, :wrong_epoch | :locked | :abort | :timeout | :unavailable}
          | {:error, {:key_out_of_range | :atomic_on_system_key, Bedrock.key()}}
          | {:error, :invalid_transaction}
  def commit(commit_proxy, epoch, transaction, opts \\ []) do
    case Keyword.get(opts, :mode, :user) do
      mode when mode in [:user, :system] -> call(commit_proxy, {:commit, epoch, transaction, mode}, :infinity)
      other -> raise ArgumentError, "invalid commit mode: #{inspect(other)}"
    end
  end
end
