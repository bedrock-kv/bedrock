defmodule Bedrock.Cluster.Gateway.TransactionBuilder do
  @moduledoc """
  Manages the complete lifecycle of individual transactions.

  Each transaction gets its own dedicated TransactionBuilder process that exists for
  the entire transaction lifetime. This provides perfect isolation between transactions
  and enables sophisticated features like nested transactions, read-your-writes
  consistency, and performance optimizations.

  ## Key Features

  ### Per-Transaction Process Model
  Each transaction runs in its own process, providing isolation and enabling complex
  state management without cross-transaction interference.

  ### Read-Your-Writes Consistency
  Maintains a local cache of writes that are immediately visible to subsequent reads
  within the same transaction, even before commit.

  ### Lazy Read Version Acquisition
  Delays acquiring read versions until the first read operation to minimize the
  conflict detection window and ensure transactions see the latest committed data.

  ### Horse Racing Performance
  Simultaneously queries multiple storage servers and uses the first successful
  response, learning which servers are fastest for future optimization.

  ### Nested Transaction Support
  Supports nested transactions where sub-transactions see parent state but maintain
  isolated changes. Nested "commits" are local merges, while rollbacks discard both
  reads and writes. Only the final, flattened top-level transaction is sent to
  commit proxies.

  ## Nested Transaction Semantics

  When a nested transaction begins:
  - It sees all reads and writes from the parent at that point
  - It gets fresh read/write maps for tracking its own changes
  - Parent state is preserved on a stack

  When a nested transaction "commits":
  - Its writes are merged into the parent transaction
  - Its reads are merged (they contributed to surviving writes)
  - This is a local operation, not a distributed commit

  When a nested transaction rolls back:
  - Both reads and writes are discarded entirely
  - Those reads "didn't happen" since they didn't contribute to final state
  - No interaction with the distributed commit system required

  Only the top-level transaction (after all nested operations resolve) is sent
  to commit proxies as a single, complete transaction.

  This design provides significant performance benefits: nested transactions require
  no network traffic, no coordination with other cluster components, and consume
  no distributed system resources. All nested operations are purely local to the
  TransactionBuilder process.
  """

  use GenServer

  import __MODULE__.Committing, only: [do_commit: 1]
  import __MODULE__.PointReads, only: [fetch_key: 2, fetch_key_selector: 2]
  import __MODULE__.Putting, only: [do_put: 3]
  import __MODULE__.RangeReads, only: [fetch_range: 4, fetch_range_selectors: 5]
  import __MODULE__.ReadVersions, only: [renew_read_version_lease: 1]
  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster.Gateway
  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutUtils
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Internal.Time

  @doc false
  @spec start_link(
          opts :: [
            gateway: Gateway.ref(),
            transaction_system_layout: Bedrock.ControlPlane.Config.TransactionSystemLayout.t(),
            read_version: non_neg_integer() | nil,
            time_fn: (-> integer())
          ]
        ) ::
          {:ok, pid()} | {:error, {:already_started, pid()}}
  def start_link(opts) do
    gateway = Keyword.fetch!(opts, :gateway)
    transaction_system_layout = Keyword.fetch!(opts, :transaction_system_layout)
    read_version = Keyword.get(opts, :read_version)
    time_fn = Keyword.get(opts, :time_fn, &Time.monotonic_now_in_ms/0)

    GenServer.start_link(
      __MODULE__,
      {gateway, transaction_system_layout, read_version, time_fn}
    )
  end

  @impl true
  def init(arg), do: {:ok, arg, {:continue, :initialization}}

  @impl true
  def handle_continue(:initialization, {gateway, transaction_system_layout}) do
    handle_continue(
      :initialization,
      {gateway, transaction_system_layout, nil, &Time.monotonic_now_in_ms/0}
    )
  end

  def handle_continue(:initialization, {gateway, transaction_system_layout, read_version}) do
    handle_continue(
      :initialization,
      {gateway, transaction_system_layout, read_version, &Time.monotonic_now_in_ms/0}
    )
  end

  def handle_continue(:initialization, {gateway, transaction_system_layout, read_version, time_fn}) do
    # Build the layout index once during initialization for O(log n) lookups
    layout_index = LayoutUtils.build_layout_index(transaction_system_layout)

    # For tests, if read_version is provided, set a far-future lease expiration
    read_version_lease_expiration =
      if read_version == nil,
        # 60 seconds from now
        do: nil,
        else: time_fn.() + 60_000

    noreply(%State{
      state: :valid,
      gateway: gateway,
      transaction_system_layout: transaction_system_layout,
      layout_index: layout_index,
      read_version: read_version,
      read_version_lease_expiration: read_version_lease_expiration
    })
  end

  def handle_continue(:stop, t), do: stop(t, :normal)

  def handle_continue(:update_version_lease_if_needed, t) when is_nil(t.read_version), do: noreply(t)

  def handle_continue(:update_version_lease_if_needed, t) do
    now = Time.monotonic_now_in_ms()
    ms_remaining = t.read_version_lease_expiration - now

    cond do
      ms_remaining <= 0 -> noreply(%{t | state: :expired})
      ms_remaining < t.lease_renewal_threshold -> t |> renew_read_version_lease() |> noreply()
      true -> noreply(t)
    end
  end

  @impl true
  def handle_call(:nested_transaction, _from, t) do
    reply(%{t | stack: [t.tx | t.stack]}, :ok)
  end

  def handle_call(:commit, _from, t) do
    case do_commit(t) do
      {:ok, t} -> reply(t, {:ok, t.commit_version}, continue: :stop)
      {:error, _reason} = error -> reply(t, error)
    end
  end

  def handle_call({:fetch, key}, _from, t) do
    case fetch_key(t, key) do
      {t, result} -> reply(t, result, continue: :update_version_lease_if_needed)
    end
  end

  def handle_call({:fetch_key_selector, key_selector}, _from, t) do
    case fetch_key_selector(t, key_selector) do
      {t, result} -> reply(t, result, continue: :update_version_lease_if_needed)
    end
  end

  def handle_call({:range_batch, start_key, end_key, batch_size, opts}, _from, t) do
    case fetch_range(t, {start_key, end_key}, batch_size, opts) do
      {t, result} -> reply(t, result, continue: :update_version_lease_if_needed)
    end
  end

  def handle_call({:range_fetch_key_selectors, start_selector, end_selector, opts}, _from, t) do
    batch_size = Keyword.get(opts, :limit, 10_000)

    case fetch_range_selectors(t, start_selector, end_selector, batch_size, opts) do
      {t, result} -> reply(t, result, continue: :update_version_lease_if_needed)
    end
  end

  @impl true
  def handle_cast({:put, key, value}, t) do
    case do_put(t, key, value) do
      {:ok, t} -> noreply(t)
      :key_error -> raise KeyError, "key must be a binary"
    end
  end

  def handle_cast(:rollback, t) do
    case do_rollback(t) do
      :stop -> noreply(t, continue: :stop)
      t -> noreply(t)
    end
  end

  @impl true
  def handle_info(:timeout, t), do: {:stop, :normal, t}

  @spec do_rollback(State.t()) :: :stop | State.t()
  def do_rollback(%{stack: []}), do: :stop
  def do_rollback(%{stack: [tx | stack]} = t), do: %{t | tx: tx, stack: stack}
end
