defmodule Bedrock.DataPlane.CommitProxy.Telemetry do
  @moduledoc false
  alias Bedrock.Telemetry

  @type telemetry_metadata :: %{optional(atom()) => term()}

  @spec trace_metadata() :: telemetry_metadata()
  def trace_metadata, do: Process.get(:trace_metadata, %{})

  @spec trace_metadata(metadata :: telemetry_metadata()) :: telemetry_metadata()
  def trace_metadata(metadata), do: Process.put(:trace_metadata, Enum.into(metadata, trace_metadata()))

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
          duration_μs :: Bedrock.interval_in_us()
        ) :: :ok
  def trace_commit_proxy_batch_finished(commit_version, n_aborts, n_oks, duration_μs) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :stop],
      %{n_oks: n_oks, n_aborts: n_aborts, duration_μs: duration_μs},
      Map.put(trace_metadata(), :commit_version, commit_version)
    )
  end

  @spec trace_commit_proxy_batch_failed(
          batch :: Bedrock.DataPlane.CommitProxy.Batch.t(),
          reason :: any(),
          duration_μs :: Bedrock.interval_in_us()
        ) :: :ok
  def trace_commit_proxy_batch_failed(batch, reason, duration_μs) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :failed],
      %{
        n_transactions: length(batch.buffer),
        duration_μs: duration_μs,
        commit_version: batch.commit_version
      },
      Map.put(trace_metadata(), :reason, reason)
    )
  end

  @spec trace_metadata_applied(count :: non_neg_integer(), families :: [atom()]) :: :ok
  def trace_metadata_applied(count, families) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :metadata_applied],
      %{count: count},
      Map.put(trace_metadata(), :families, families)
    )
  end

  @doc """
  A transaction was rejected at ingress because its mutation section raised
  during validation. Fail-closed is correct, but a burst of these can also
  mean a validator bug - surface it.
  """
  @spec trace_ingress_validation_failed(error :: term()) :: :ok
  def trace_ingress_validation_failed(error) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :ingress_validation_failed],
      %{count: 1},
      Map.put(trace_metadata(), :error, error)
    )
  end

  @spec trace_unknown_key_skipped(keys :: [Bedrock.key()]) :: :ok
  def trace_unknown_key_skipped(keys) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :unknown_key_skipped],
      %{count: length(keys)},
      Map.put(trace_metadata(), :keys, keys)
    )
  end

  @spec trace_metadata_updates_received(
          commit_version :: Bedrock.version(),
          metadata_updates :: [term()]
        ) :: :ok
  def trace_metadata_updates_received(commit_version, metadata_updates) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :metadata_updates_received],
      %{n_updates: length(metadata_updates)},
      Map.merge(trace_metadata(), %{commit_version: commit_version, metadata_updates: metadata_updates})
    )
  end
end
