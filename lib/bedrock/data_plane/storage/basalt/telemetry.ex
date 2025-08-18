defmodule Bedrock.DataPlane.Storage.Basalt.Telemetry do
  @moduledoc false
  alias Bedrock.Telemetry

  @spec trace_log_pull_start(Bedrock.version(), Bedrock.version()) :: :ok
  def trace_log_pull_start(timestamp_version, next_version) do
    Telemetry.execute([:bedrock, :storage, :pull_start], %{}, %{
      timestamp: timestamp_version,
      next_version: next_version
    })
  end

  @spec trace_log_pull_succeeded(Bedrock.version(), non_neg_integer()) :: :ok
  def trace_log_pull_succeeded(timestamp_version, n_transactions) do
    Telemetry.execute([:bedrock, :storage, :pull_succeeded], %{}, %{
      timestamp: timestamp_version,
      n_transactions: n_transactions
    })
  end

  @spec trace_log_pull_failed(Bedrock.version(), term()) :: :ok
  def trace_log_pull_failed(timestamp_version, reason) do
    Telemetry.execute([:bedrock, :storage, :pull_failed], %{}, %{
      timestamp: timestamp_version,
      reason: reason
    })
  end

  @spec trace_log_marked_as_failed(Bedrock.version(), term()) :: :ok
  def trace_log_marked_as_failed(timestamp_version, log_id) do
    Telemetry.execute([:bedrock, :storage, :log_marked_as_failed], %{}, %{
      timestamp: timestamp_version,
      log_id: log_id
    })
  end

  @spec trace_log_pull_circuit_breaker_tripped(Bedrock.version(), pos_integer()) :: :ok
  def trace_log_pull_circuit_breaker_tripped(timestamp_version, ms_to_wait) do
    Telemetry.execute([:bedrock, :storage, :log_pull_circuit_breaker_tripped], %{}, %{
      timestamp: timestamp_version,
      ms_to_wait: ms_to_wait
    })
  end

  @spec trace_log_pull_circuit_breaker_reset(Bedrock.version()) :: :ok
  def trace_log_pull_circuit_breaker_reset(timestamp_version) do
    Telemetry.execute([:bedrock, :storage, :log_pull_circuit_breaker_reset], %{}, %{
      timestamp: timestamp_version
    })
  end
end
