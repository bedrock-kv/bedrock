defmodule Bedrock.DataPlane.Log do
  @moduledoc """
  """

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Worker

  @type ref :: Worker.ref()
  @type id :: Worker.id()
  @type health :: Worker.health()
  @type fact_name ::
          Worker.fact_name()
          | :last_tx_id
          | :oldest_tx_id
          | :minimum_durable_tx_id

  @doc """
  Apply a new mutation to the log. The previous transaction id is given as a
  check to ensure strict ordering. If the previous transaction id is not the
  latest transaction, the mutation will be rejected.
  """
  @spec push(log :: ref(), Transaction.t(), last_commit_version :: Bedrock.version()) ::
          :ok | {:error, :tx_out_of_order | :locked | :unavailable}
  def push(log, transaction, last_commit_version) do
    GenServer.call(log, {:push, transaction, last_commit_version})
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @doc """
  Retrieve up to `demand` transactions, starting immediately after the given
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
          log :: ref(),
          last_tx_id :: Bedrock.version(),
          demand :: pos_integer(),
          opts :: [
            subscriber_id: String.t(),
            last_durable_tx_id: Bedrock.version()
          ]
        ) ::
          {:ok, [Transaction.t()]} | {:error, :not_ready | :tx_too_new | :unavailable}
  def pull(log, last_tx_id, demand, opts) do
    GenServer.call(log, {:pull, last_tx_id, demand, opts})
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
  @spec lock_for_recovery(log :: ref(), Bedrock.epoch()) ::
          {:ok, pid(), recovery_info :: keyword()} | {:error, :newer_epoch_exists}
  defdelegate lock_for_recovery(storage, epoch), to: Worker

  @doc """
  Ask the transaction log worker for various facts about itself.
  """
  @spec info(storage :: ref(), [fact_name()], opts :: keyword()) ::
          {:ok, keyword()} | {:error, term()}
  defdelegate info(storage, fact_names, opts \\ []), to: Worker
end
