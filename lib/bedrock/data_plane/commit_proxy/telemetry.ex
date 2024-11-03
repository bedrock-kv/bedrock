defmodule Bedrock.DataPlane.CommitProxy.Telemetry do
  alias Bedrock.Telemetry
  alias Bedrock.DataPlane.CommitProxy.Batch

  @spec trace_commit_proxy_batch_succeeded(
          cluster :: module(),
          pid :: pid(),
          batch :: Batch.t()
        ) :: :ok
  def trace_commit_proxy_batch_succeeded(cluster, pid, batch) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :batch_failure],
      %{n_transactions: length(batch.buffer)},
      %{cluster: cluster, pid: pid, commit_version: batch.commit_version}
    )
  end

  @spec trace_commit_proxy_batch_failure(
          cluster :: module(),
          pid :: pid(),
          batch :: Batch.t(),
          reason :: any()
        ) :: :ok
  def trace_commit_proxy_batch_failure(cluster, pid, batch, reason) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :batch_failure],
      %{n_transactions: length(batch.buffer)},
      %{cluster: cluster, pid: pid, reason: reason}
    )
  end
end
