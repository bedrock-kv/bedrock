defmodule Bedrock.JobQueue.Consumer.LeaseExtender do
  @moduledoc """
  Periodically extends a lease while a job is being processed.

  Per QuiCK paper Algorithm 3: Workers should extend leases in parallel with
  job execution to prevent long-running jobs from losing their lease.

  The extender runs until explicitly stopped via `stop/1` or the process is killed.
  Extension failures are logged but don't stop the extender - the job may still
  complete in time.
  """

  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.Store

  require Logger

  @doc """
  Starts a lease extender process.

  Options:
  - `:interval` - How often to extend the lease in ms (default: lease_duration / 3)
  - `:extension` - How much to extend by in ms (default: lease_duration)

  Returns the pid of the extender process.
  """
  @spec start(module(), term(), Lease.t(), pos_integer(), keyword()) :: pid()
  def start(repo, root, lease, lease_duration, opts \\ []) do
    interval = Keyword.get(opts, :interval, div(lease_duration, 3))
    extension = Keyword.get(opts, :extension, lease_duration)

    spawn_link(fn ->
      loop(repo, root, lease, interval, extension)
    end)
  end

  @doc """
  Stops a lease extender process.
  """
  @spec stop(pid()) :: :ok
  def stop(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      send(pid, :stop)
    end

    :ok
  end

  # Main loop - waits for interval, extends lease, repeats
  defp loop(repo, root, lease, interval, extension) do
    receive do
      :stop ->
        :ok
    after
      interval ->
        lease = extend_lease(repo, root, lease, extension)
        loop(repo, root, lease, interval, extension)
    end
  end

  # Extends the lease, returns updated lease or original on failure
  defp extend_lease(repo, root, lease, extension) do
    result =
      repo.transact(fn ->
        Store.extend_lease(repo, root, lease, extension)
      end)

    case result do
      {:ok, {:ok, updated_lease}} ->
        Logger.debug("Extended lease for item #{Base.encode16(lease.item_id, case: :lower)}")
        updated_lease

      {:ok, {:error, reason}} ->
        Logger.warning(
          "Failed to extend lease for item #{Base.encode16(lease.item_id, case: :lower)}: #{inspect(reason)}"
        )

        lease

      {:error, reason} ->
        Logger.warning(
          "Transaction failed extending lease for item #{Base.encode16(lease.item_id, case: :lower)}: #{inspect(reason)}"
        )

        lease
    end
  end
end
