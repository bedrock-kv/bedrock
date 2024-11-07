defmodule Bedrock.DataPlane.CommitProxy.Finalization do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Worker

  import Bedrock.DataPlane.Resolver, only: [resolve_transactions: 5]

  import Bedrock.DataPlane.CommitProxy.Batch,
    only: [transactions_in_order: 1]

  @doc """
  Finalizes a batch of transactions by resolving conflicts, separating
  successful transactions from aborts, and pushing them to the log servers.

  This function processes a batch of transactions, first ensuring that any
  conflicts are resolved. After conflict resolution, it organizes the
  transactions into those that will be committed and those that will be aborted.

  Clients with aborted transactions are notified of the abort immediately.
  Successful transactions are pushed to the system's logs, and clients that
  submitted the transactions are notified when a majority of the log servers
  have acknowledged.

  ## Parameters

    - `batch`: A `Batch.t()` struct that contains the transactions to be finalized,
      along with the commit version details.
    - `transaction_system_layout`: Provides configuration and systemic details,
      including the available resolver and log servers.

  ## Returns
    - `:ok` when the batch has been processed, and all clients have been
      notified about the status of their transactions.
  """
  @spec finalize_batch(Batch.t(), TransactionSystemLayout.t()) :: :ok | {:error, term()}
  def finalize_batch(batch, transaction_system_layout) do
    transactions_in_order = transactions_in_order(batch)

    commit_version = batch.commit_version

    with {:ok, aborted} <-
           resolve_transactions(
             transaction_system_layout.resolvers |> List.first(),
             batch.last_commit_version,
             commit_version,
             transform_transactions_for_resolution(transactions_in_order),
             timeout: 1_000
           ),
         {oks, aborts, compacted_transaction} <-
           prepare_transaction_to_log(
             transactions_in_order,
             aborted,
             commit_version
           ),
         :ok <- reply_to_all_clients_with_aborted_transactions(aborts),
         :ok <-
           push_transaction_to_logs(
             transaction_system_layout,
             batch.last_commit_version,
             compacted_transaction,
             fn version ->
               send_reply_with_commit_version(oks, version)
             end
           ) do
      :ok
    else
      {:error, _reason} = error ->
        batch
        |> Batch.all_callers()
        |> reply_to_all_clients_with_aborted_transactions()

        error
    end
  end

  @spec determine_majority(n :: non_neg_integer()) :: non_neg_integer()
  defp determine_majority(n), do: 1 + div(n, 2)

  @doc """
  Pushes a transaction to the logs and waits for acknowledgement from a
  majority of log servers.

  This function takes a transaction and tries to send it to all available
  log servers. It uses asynchronous tasks to push the transaction to each
  log server, and will consider the push successful once a majority of the
  servers confirm successful acceptance of the transaction.

  Once a majority of logs have acknowledged the push, the function will
  send an acknowledgement of success to the clients that initiated the
  transactions.

  ## Parameters

    - `transaction_system_layout`: Contains configuration information about the
      transaction system, including available log servers.
    - `last_commit_version`: The last known committed version; used to
      ensure consistency in log ordering.
    - `transaction`: The transaction to be committed to logs; this should
      include both data and a new commit version.
    - `oks`: A list of reply functions to which successful acknowledgements
      should be sent once enough logs have acknowledged.

  ## Returns
    - `:ok` if enough acknowledgements have been received from the log servers.
    - `:error` if a majority of logs have not successfully acknowledged the
       push within the timeout period.
  """
  @spec push_transaction_to_logs(
          TransactionSystemLayout.t(),
          last_commit_version :: Bedrock.version(),
          Transaction.t(),
          majority_reached :: (Bedrock.version() -> :ok)
        ) :: :ok
  def push_transaction_to_logs(
        transaction_system_layout,
        last_commit_version,
        transaction,
        majority_reached
      ) do
    commit_version = Transaction.version(transaction)

    log_descriptors = transaction_system_layout.logs
    n = map_size(log_descriptors)
    m = determine_majority(n)

    log_descriptors
    |> resolve_log_descriptors(transaction_system_layout.services)
    |> Task.async_stream(
      fn %{id: log_id} = service_descriptor ->
        service_descriptor
        |> try_to_push_transaction_to_log(transaction, last_commit_version)
        |> then(&{log_id, &1})
      end,
      timeout: 5_000
    )
    |> Enum.reduce_while(0, fn
      {:ok, {_log_id, {:error, _reason}}}, count ->
        {:cont, count}

      {:ok, {_log_id, :ok}}, count ->
        count = 1 + count

        if count == m do
          :ok = majority_reached.(commit_version)
        end

        {:cont, count}
    end)
    |> case do
      ^n -> :ok
      # If we haven't received enough responses, we need to abort
      _ -> :error
    end
  end

  @spec resolve_log_descriptors(
          %{Log.id() => LogDescriptor.t()},
          %{Worker.id() => ServiceDescriptor.t()}
        ) ::
          %{Worker.id() => ServiceDescriptor.t()}
  def resolve_log_descriptors(log_descriptors, services) do
    log_descriptors
    |> Map.keys()
    |> Enum.map(&Map.get(services, &1))
    |> Enum.reject(&is_nil/1)
    |> Map.new(&{&1.id, &1})
  end

  @spec try_to_push_transaction_to_log(ServiceDescriptor.t(), Transaction.t(), Bedrock.version()) ::
          :ok | {:error, :unavailable}
  def try_to_push_transaction_to_log(
        %{kind: :log, status: {:up, log_server}},
        transaction,
        last_commit_version
      ) do
    Log.push(log_server, transaction, last_commit_version)
  end

  def try_to_push_transaction_to_log(_, _, _), do: {:error, :unavailable}

  @spec reply_to_all_clients_with_aborted_transactions([Batch.reply_fn()]) :: :ok
  def reply_to_all_clients_with_aborted_transactions([]), do: :ok

  def reply_to_all_clients_with_aborted_transactions(aborts),
    do: Enum.each(aborts, & &1.({:error, :aborted}))

  @spec send_reply_with_commit_version([Batch.reply_fn()], Bedrock.version()) ::
          :ok
  def send_reply_with_commit_version(oks, commit_version),
    do: Enum.each(oks, & &1.({:ok, commit_version}))

  @doc """
  Prepare a transaction for logging by separating successful transactions
  from aborted ones and consolidating writes. Since we've completed conflict
  resolution, we can drop the read data and only keep the writes.

  For transactions without any aborts, it efficiently aggregates all writes and
  acknowledges all clients with successful executions.

  In the presence of aborted transactions, it identifies and separates them,
  ensuring only the successful transactions' writes are aggregated, and replies
  to the relevant clients about the aborts.

  Returns a tuple with:
    - A list of reply functions for successful transactions.
    - A list of reply functions for aborted transactions.
    - A Transaction.t() containing the commit version along with the aggregated
      writes from the successful transactions.

  ## Parameters

    - `transactions`: A list of transactions, each containing the reply
      function, read/write data, and other necessary details.
    - `aborts`: A list of integer indices indicating which transactions were
      aborted.
    - `commit_version`: The current commit version.

  ## Returns
    - A tuple: `{oks, aborts, transaction_to_log}`
  """
  @spec prepare_transaction_to_log(
          transactions :: [Bedrock.transaction()],
          aborts :: [integer()],
          commit_version :: Bedrock.version()
        ) ::
          {oks :: [Batch.reply_fn()], aborts :: [Batch.reply_fn()], Transaction.t()}
  # If there are no aborted transactions, we can make take some shortcuts.
  def prepare_transaction_to_log(transactions, [], commit_version) do
    transactions
    |> Enum.reduce({[], %{}}, fn {from, {_, _, writes}}, {oks, all_writes} ->
      {[from | oks], Map.merge(all_writes, writes)}
    end)
    |> then(fn {oks, combined_writes} ->
      {oks, [], Transaction.new(commit_version, combined_writes)}
    end)
  end

  # If there are aborted transactions, we need to pluck them out so that they
  # can be informed of the failure.
  def prepare_transaction_to_log(transactions, aborts, commit_version) do
    aborted_set = MapSet.new(aborts)

    transactions
    |> Enum.with_index()
    |> Enum.reduce({[], [], %{}}, fn
      {{from, {_, _, writes}}, idx}, {oks, aborts, all_writes} ->
        if MapSet.member?(aborted_set, idx) do
          {oks, [from | aborts], all_writes}
        else
          {[from | oks], aborts, Map.merge(all_writes, writes)}
        end
    end)
    |> then(fn {oks, aborts, combined_writes} ->
      {oks, aborts, Transaction.new(commit_version, combined_writes)}
    end)
  end

  @doc """
  Transforms the list of transactions for resolution.

  Converts the transaction data to the format expected by the conflict
  resolution logic. For each transaction, it extracts the read version,
  the reads, and the keys of the writes, discarding the values of the writes
  as they are not needed for resolution.

  ## Parameters

    - `transactions`: A list of transactions where each transaction is
      represented as a tuple containing the process `from` identifier,
      read version, read data, and write data.

  ## Returns
    - A list of transformed transactions as tuples, each containing:
      - The read version
      - The read data
      - The keys of the write data
  """
  @spec transform_transactions_for_resolution([Bedrock.transaction()]) :: [Resolver.transaction()]
  def transform_transactions_for_resolution(transactions) do
    transactions
    |> Enum.map(fn {_from, {read_version, reads, writes}} ->
      {read_version, reads, writes |> Map.keys()}
    end)
  end
end
