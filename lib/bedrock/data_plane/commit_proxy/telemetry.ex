defmodule Bedrock.DataPlane.CommitProxy.Telemetry do
  alias Bedrock.Telemetry
  alias Bedrock.DataPlane.CommitProxy.Batch

  def trace_metadata, do: Process.get(:trace_metadata, %{})

  def trace_metadata(metadata),
    do: Process.put(:trace_metadata, Enum.into(metadata, trace_metadata()))

  @spec trace_commit_proxy_batch_started(
          commit_version :: Bedrock.version(),
          n_transactions :: non_neg_integer(),
          started_at :: Bedrock.timestamp_in_ms()
        ) :: :ok
  def trace_commit_proxy_batch_started(commit_version, n_transactions, started_at) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :start],
      %{n_transactions: n_transactions},
      Map.merge(trace_metadata(), %{commit_version: commit_version, started_at: started_at})
    )
  end

  @spec trace_commit_proxy_batch_finished(
          commit_version :: Bedrock.version(),
          n_aborts :: non_neg_integer(),
          n_oks :: non_neg_integer(),
          duration_us :: Bedrock.interval_in_us()
        ) :: :ok
  def trace_commit_proxy_batch_finished(commit_version, n_aborts, n_oks, duration_us) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :stop],
      %{n_oks: n_oks, n_aborts: n_aborts, duration_us: duration_us},
      Map.merge(trace_metadata(), %{commit_version: commit_version})
    )
  end

  @spec trace_commit_proxy_batch_failed(
          batch :: Batch.t(),
          reason :: any(),
          duration_us :: Bedrock.interval_in_us()
        ) :: :ok
  def trace_commit_proxy_batch_failed(batch, reason, duration_us) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :failed],
      %{n_transactions: length(batch.buffer), duration_us: duration_us},
      Map.merge(trace_metadata(), %{reason: reason})
    )
  end
end
