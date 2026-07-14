defmodule Bedrock.ControlPlane.Distributor do
  @moduledoc """
  The distributor is a cluster singleton that owns shard-coverage policy:
  on-demand materializer recruitment, placeholder supervision, and
  materializer-death healing (with shard split/merge and rebalancing to
  follow). It is the analogue of FoundationDB's Data Distributor.

  ## Lifecycle

  The distributor is recruited by the director *after* recovery completes
  (FDB: the cluster controller recruits the data distributor once the
  cluster is accepting commits) and is handed the current epoch along with
  a snapshot of the shard layout. The director monitors the distributor
  and re-recruits it if it dies.

  The distributor is deliberately off the request path: its death degrades
  coverage healing but never blocks reads or writes.

  ## Epoch Validation

  Every shard-map mutation the distributor initiates is epoch-validated
  (the analogue of FDB's `moveKeysLock`). When the distributor learns that
  the cluster has moved to a newer epoch - via `check_epoch/3` from a
  caller carrying a newer epoch, or via `notify_epoch_change/2` - it stops
  itself with reason `:normal`, ceding to the distributor of the new epoch.
  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @doc """
  Validates the caller's epoch against the distributor's own epoch.

  This is the guard that all shard-map mutations pass through. Returns
  `:ok` when the epochs match. If the caller carries a *newer* epoch, the
  distributor has been superseded: it replies `{:error, :epoch_superseded}`
  and stops itself with reason `:normal`. If the caller carries an *older*
  epoch, the caller is stale and receives `{:error, :newer_epoch_exists}`.
  """
  @spec check_epoch(ref(), Bedrock.epoch(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          :ok
          | {:error, :epoch_superseded | :newer_epoch_exists}
          | {:error, :unavailable | :timeout | :unknown}
  def check_epoch(distributor, epoch, opts \\ []),
    do: call(distributor, {:check_epoch, epoch}, opts[:timeout_in_ms] || 5_000)

  @doc """
  Notifies the distributor that the cluster epoch has changed.

  If the new epoch is greater than the distributor's own epoch, the
  distributor stops itself with reason `:normal` (a newer epoch's director
  will recruit its own distributor). Signals for the same or an older
  epoch are ignored.
  """
  @spec notify_epoch_change(ref(), Bedrock.epoch()) :: :ok
  def notify_epoch_change(distributor, new_epoch), do: cast(distributor, {:epoch_changed, new_epoch})
end
