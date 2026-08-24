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
  Fetches the covering routing entry for one key: the shard's bounds,
  its tag, and the raw materializer ref.

  This is FDB's `GetKeyServerLocations`, answered per key from the
  proxy's live routing view - a ceiling walk, never a bulk projection of
  a map that can number in the thousands. The answer is at least as
  fresh as the proxy's most recently applied commit, unversioned by
  design. Locations are unverified hints; staleness costs the caller a
  retry, never a wrong answer. `{:error, :not_found}` means the
  committed state routes the key nowhere - to the client, an unroutable
  key.

  A locked proxy replies `{:error, :locked}`: FDB parks location requests
  until its state is valid, Bedrock refuses and lets the client's retry
  loop be the parking lot.
  """
  @spec fetch_routing(commit_proxy_ref :: ref(), Bedrock.key(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, RoutingData.covering_entry()}
          | {:error, :not_found | :locked | :timeout | :unavailable}
  def fetch_routing(commit_proxy, key, opts \\ []),
    do: call(commit_proxy, {:fetch_routing, key}, opts[:timeout_in_ms] || 5_000)

  @doc """
  Resolves the committed materializer assignment for one shard tag.

  This is the rejoin-validation ask (FDB's storage-server rejoin through a
  commit proxy's txnStateStore): a materializer checks whether the
  `materializers/<tag>` entry still names it. `{:error, :not_found}` is an
  authoritative answer — the committed keyspace names no materializer for
  the tag. A locked proxy replies `{:error, :locked}`; callers treat that
  (and unavailability) as "ask again later", never as displacement.
  """
  @spec resolve_materializer(
          commit_proxy_ref :: ref(),
          tag :: non_neg_integer(),
          opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]
        ) ::
          {:ok, {worker_id :: String.t(), node :: String.t()}}
          | {:error, :not_found | :locked | :timeout | :unavailable}
  def resolve_materializer(commit_proxy, tag, opts \\ []),
    do: call(commit_proxy, {:resolve_materializer, tag}, opts[:timeout_in_ms] || 5_000)

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
  """
  @spec commit(
          commit_proxy_ref :: ref(),
          epoch :: Bedrock.epoch(),
          transaction :: Transaction.encoded(),
          opts :: [mode: :user | :system, timeout_in_ms: Bedrock.timeout_in_ms()]
        ) ::
          {:ok, version :: Bedrock.version(), index :: non_neg_integer()}
          | {:error, :wrong_epoch | :locked | :aborted | :timeout | :unavailable}
          | {:error, {:key_out_of_range, Bedrock.key()}}
          | {:error, :invalid_transaction}
  def commit(commit_proxy, epoch, transaction, opts \\ []) do
    timeout = Keyword.get(opts, :timeout_in_ms, :infinity)

    case Keyword.get(opts, :mode, :user) do
      mode when mode in [:user, :system] -> call(commit_proxy, {:commit, epoch, transaction, mode}, timeout)
      other -> raise ArgumentError, "invalid commit mode: #{inspect(other)}"
    end
  end
end
