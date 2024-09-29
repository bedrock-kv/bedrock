defmodule Bedrock.DataPlane.TransactionLog do
  use Bedrock, :types
  use Bedrock.Cluster, :types

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Worker

  @type t :: Worker.t()
  @type id :: Worker.id()
  @type health :: :ok | {:error, term()}
  @type fact_name ::
          Worker.fact_name()
          | :last_tx_id
          | :minimum_durable_tx_id

  @doc """
  Apply a new mutation to the log. The previous transaction id is given as a
  check to ensure strict ordering. If the previous transaction id is not the
  latest transaction, the mutation will be rejected.
  """
  @spec push(transaction_log :: t(), Transaction.t(), prev_tx_id :: Transaction.version()) ::
          :ok | {:error, :tx_out_of_order | :locked | :unavailable}
  def push(transaction_log, transaction, prev_tx_id) do
    GenServer.call(transaction_log, {:push, transaction, prev_tx_id})
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @doc """
  Retrieve up to `count` transactions, starting immediately after the given
  `last_tx_id`.

  If the `subscriber_id` option is present, we'll make a note of the
  `last_tx_id` (and `lase_durable_tx_id` if present) for that subscriber along
  with a timestamp of the pull.

  (It is expected that storage workers will supply their id as the
  `subscriber_id` and report their `last_durable_tx_id` when they pull. Other
  clients of the log may not.)

  ## Errors:
  -- `:not_ready`: The worker is not ready to serve transactions.
  -- `:tx_too_new`: The `last_tx_id` is newer than the latest
     transaction.
  """
  @spec pull(
          transaction_log :: t(),
          last_tx_id :: Transaction.version(),
          count :: pos_integer(),
          opts :: [
            subscriber_id: String.t(),
            last_durable_tx_id: Transaction.version()
          ]
        ) ::
          {:ok, [] | [Transaction.t()]} | {:error, :not_ready | :tx_too_new | :unavailable}
  def pull(transaction_log, last_tx_id, count, opts) do
    GenServer.call(transaction_log, {:pull, last_tx_id, count, opts})
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @doc """
  Request that the transaction log worker lock itself and stop accepting new
  transactions. This mechanism is used by a newly elected cluster controller
  to prevent new transactions from being accepted while it is establishing
  its authority.

  In order for the lock to succeed, the given epoch needs to be greater than
  the current epoch.
  """
  @spec lock(transaction_log :: t(), cluster_controller :: pid(), epoch()) :: :ok
  def lock(transaction_log, cluster_controller, epoch),
    do: GenServer.cast(transaction_log, {:lock, cluster_controller, epoch})

  @doc """
  Ask the transaction log worker for various facts about itself.
  """
  @spec info(transaction_log :: t(), [fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(transaction_log :: t(), [fact_name()], timeout_in_ms()) ::
          {:ok, keyword()} | {:error, term()}
  defdelegate info(transaction_log, fact_names, timeout \\ 5_000), to: Worker
end
