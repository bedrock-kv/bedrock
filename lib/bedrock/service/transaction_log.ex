defmodule Bedrock.Service.TransactionLog do
  use Bedrock, :types
  use Bedrock.Cluster, :types

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Worker

  @type worker :: Worker.worker()
  @type id :: String.t()
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
  @spec push(worker(), Transaction.t(), prev_tx_id :: Transaction.version()) ::
          :ok
          | {:error, :out_of_order | :locked}
  def push(worker, transaction, prev_tx_id),
    do: GenServer.call(worker, {:push, transaction, prev_tx_id})

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
          worker :: worker(),
          last_tx_id :: Transaction.version(),
          count :: pos_integer(),
          opts :: [
            subscriber_id: String.t(),
            last_durable_tx_id: Transaction.version()
          ]
        ) ::
          {:ok, [] | [Transaction.t()]} | {:error, :not_ready | :tx_too_new}
  def pull(worker, last_tx_id, count, opts),
    do: GenServer.call(worker, {:pull, last_tx_id, count, opts})

  @doc """
  Request that the transaction log worker lock itself and stop accepting new
  transactions. This mechanism is used by a newly elected cluster controller
  to prevent new transactions from being accepted while it is establishing
  its authority.

  In order for the lock to succeed, the given epoch needs to be greater than
  the current epoch.
  """
  @spec lock(worker(), cluster_controller :: pid(), epoch()) :: :ok
  def lock(worker, cluster_controller, epoch),
    do: GenServer.cast(worker, {:lock, cluster_controller, epoch})

  @doc """
  Ask the transaction log worker for various facts about itself.
  """
  @spec info(worker(), [fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(worker(), [fact_name()], timeout_in_ms()) :: {:ok, keyword()} | {:error, term()}
  defdelegate info(worker, fact_names, timeout \\ 5_000),
    to: Bedrock.Service.Worker
end
