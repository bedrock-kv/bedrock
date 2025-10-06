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

  import __MODULE__.Finalization, only: [commit: 1, rollback: 1]
  import __MODULE__.PointReads, only: [get_key: 3, get_key_selector: 3]
  import __MODULE__.RangeReads, only: [get_range: 4, get_range_selectors: 5]
  import Bedrock.Internal.GenServer.Replies

  alias __MODULE__.Tx
  alias Bedrock.Cluster.Gateway
  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.KeySelector

  @doc false
  @spec start_link(
          opts :: [
            gateway: Gateway.ref(),
            transaction_system_layout: TransactionSystemLayout.t(),
            time_fn: (-> integer())
          ]
        ) ::
          {:ok, pid()} | {:error, {:already_started, pid()}}
  def start_link(opts) do
    gateway = Keyword.fetch!(opts, :gateway)
    transaction_system_layout = Keyword.fetch!(opts, :transaction_system_layout)

    GenServer.start_link(
      __MODULE__,
      {gateway, transaction_system_layout}
    )
  end

  @impl true
  def init(arg), do: {:ok, arg, {:continue, :initialization}}

  @impl true
  def handle_continue(:initialization, {gateway, transaction_system_layout}) do
    # Build the layout index once during initialization for O(log n) lookups
    layout_index = LayoutIndex.build_index(transaction_system_layout)

    noreply(%State{
      state: :valid,
      gateway: gateway,
      transaction_system_layout: transaction_system_layout,
      layout_index: layout_index,
      read_version: nil
    })
  end

  def handle_continue(:stop, t), do: stop(t, :normal)

  @impl true
  def handle_call(:nested_transaction, _from, t), do: reply(%{t | stack: [t.tx | t.stack]}, :ok)

  def handle_call(:commit, _from, t) do
    t
    |> commit()
    |> case do
      {:ok, new_t} when t.stack == [] ->
        # Outermost transaction committed - stop the process
        reply(new_t, {:ok, new_t.commit_version}, continue: :stop)

      {:ok, new_t} ->
        # Nested transaction committed (stack popped) - continue
        reply(new_t, :ok)

      {:error, _reason} = error ->
        reply(t, error)
    end
  end

  def handle_call({:get, key}, from, t) when is_binary(key), do: handle_call({:get, key, []}, from, t)

  def handle_call({:get, key, opts}, _from, t) when is_binary(key) and is_list(opts) do
    t
    |> get_key(key, opts)
    |> then(fn
      {t, {:error, _} = error} ->
        reply(t, error)

      {t, {:failure, failures_by_reason}} ->
        reply(t, {:failure, choose_a_reason(failures_by_reason)})

      {t, {:ok, {^key, value}}} ->
        reply(t, {:ok, value})
    end)
  end

  def handle_call({:get_key_selector, %KeySelector{} = key_selector, opts}, _from, t) do
    t
    |> get_key_selector(key_selector, opts)
    |> then(fn
      {t, {:failure, failures_by_reason}} ->
        reply(t, {:failure, choose_a_reason(failures_by_reason)})

      {t, result} ->
        reply(t, result)
    end)
  end

  def handle_call({:get_range, start_key, end_key, batch_size, opts}, _from, t)
      when is_binary(start_key) and is_binary(end_key) do
    t
    |> get_range({start_key, end_key}, batch_size, opts)
    |> then(fn
      {t, {:failure, failures_by_reason}} ->
        reply(t, {:failure, choose_a_reason(failures_by_reason)})

      {t, result} ->
        reply(t, result)
    end)
  end

  def handle_call(
        {:get_range_selectors, %KeySelector{} = start_selector, %KeySelector{} = end_selector, opts},
        _from,
        t
      ) do
    batch_size = Keyword.get(opts, :limit, 10_000)

    t
    |> get_range_selectors(start_selector, end_selector, batch_size, opts)
    |> then(fn
      {t, {:failure, failures_by_reason}} ->
        reply(t, {:failure, choose_a_reason(failures_by_reason)})

      {t, result} ->
        reply(t, result)
    end)
  end

  @impl true
  def handle_cast({:set_key, key, value}, t) when is_binary(key) and is_binary(value),
    do: noreply(%{t | tx: Tx.set(t.tx, key, value, [])})

  def handle_cast({:set_key, key, nil, opts}, t) when is_binary(key), do: noreply(%{t | tx: Tx.clear(t.tx, key, opts)})

  def handle_cast({:set_key, key, value, opts}, t) when is_binary(key) and is_binary(value),
    do: noreply(%{t | tx: Tx.set(t.tx, key, value, opts)})

  def handle_cast({:atomic, op, key, value}, t) when is_atom(op) and is_binary(key) and is_binary(value),
    do: noreply(%{t | tx: Tx.atomic_operation(t.tx, key, op, value)})

  def handle_cast({:clear, key, opts}, t) when is_binary(key), do: noreply(%{t | tx: Tx.clear(t.tx, key, opts)})

  def handle_cast({:clear_range, start_key, end_key, opts}, t) when is_binary(start_key) and is_binary(end_key),
    do: noreply(%{t | tx: Tx.clear_range(t.tx, start_key, end_key, opts)})

  def handle_cast({:add_read_conflict_key, key}, t) when is_binary(key),
    do: noreply(%{t | tx: Tx.add_read_conflict_key(t.tx, key)})

  def handle_cast({:add_write_conflict_range, start_key, end_key}, t) when is_binary(start_key) and is_binary(end_key),
    do: noreply(%{t | tx: Tx.add_write_conflict_range(t.tx, start_key, end_key)})

  def handle_cast(:rollback, t) do
    t
    |> rollback()
    |> then(fn
      :stop -> noreply(t, continue: :stop)
      t -> noreply(t)
    end)
  end

  @impl true
  def handle_info(:timeout, t), do: {:stop, :normal, t}

  defp choose_a_reason(%{timeout: _}), do: :timeout
  defp choose_a_reason(%{unavailable: _}), do: :unavailable
  defp choose_a_reason(%{version_too_new: _}), do: :version_too_new
  defp choose_a_reason(failures_by_reason), do: failures_by_reason |> Map.keys() |> hd()
end
