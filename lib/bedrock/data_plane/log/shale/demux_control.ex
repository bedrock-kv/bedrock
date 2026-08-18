defmodule Bedrock.DataPlane.Log.Shale.DemuxControl do
  @moduledoc false

  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log.Shale.State

  # The single place a log's Demux is started from — initialization and
  # recovery reset must agree on every option (a bucket-width drift across
  # recovery would break chunk-content determinism against pre-crash chunks).
  @spec start(State.t()) :: {:ok, pid()} | {:error, term()}
  def start(t) do
    Demux.Server.start_link(
      cluster: t.cluster,
      object_storage: t.object_storage,
      log: self()
    )
  end

  @doc """
  Synchronously tears down a Demux tree (Demux, ShardServers, persistence
  workers). If the graceful stop times out, the tree is killed and awaited —
  proceeding while a stale flush pipeline might still be alive is never
  acceptable.
  """
  @spec teardown(pid() | nil) :: :ok
  def teardown(nil), do: :ok

  def teardown(demux) do
    Process.unlink(demux)

    try do
      GenServer.stop(demux, :shutdown, 10_000)
    catch
      :exit, _ -> kill_and_await(demux)
    end

    :ok
  end

  @spec kill_and_await(pid()) :: :ok
  def kill_and_await(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      5_000 -> :ok
    end
  end
end
