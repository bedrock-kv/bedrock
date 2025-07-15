defmodule Bedrock.DataPlane.Storage.Basalt.Telemetry do
  alias Bedrock.Telemetry

  @spec trace_log_pull_start(integer(), Bedrock.version()) :: :ok
  def trace_log_pull_start(timestamp, next_version) do
    Telemetry.execute([:bedrock, :storage, :pull_start], %{}, %{
      timestamp: timestamp,
      next_version: next_version
    })
  end

  @spec trace_log_pull_succeeded(integer(), non_neg_integer()) :: :ok
  def trace_log_pull_succeeded(timestamp, n_transactions) do
    Telemetry.execute([:bedrock, :storage, :pull_succeeded], %{}, %{
      timestamp: timestamp,
      n_transactions: n_transactions
    })
  end

  @spec trace_log_pull_failed(integer(), term()) :: :ok
  def trace_log_pull_failed(timestamp, reason) do
    Telemetry.execute([:bedrock, :storage, :pull_failed], %{}, %{
      timestamp: timestamp,
      reason: reason
    })
  end

  @spec trace_log_marked_as_failed(DateTime.t(), term()) :: :ok
  def trace_log_marked_as_failed(timestamp, log_id) do
    Telemetry.execute([:bedrock, :storage, :log_marked_as_failed], %{}, %{
      timestamp: timestamp,
      log_id: log_id
    })
  end

  @spec trace_log_pull_circuit_breaker_tripped(integer(), pos_integer()) :: :ok
  def trace_log_pull_circuit_breaker_tripped(timestamp, ms_to_wait) do
    Telemetry.execute([:bedrock, :storage, :log_pull_circuit_breaker_tripped], %{}, %{
      timestamp: timestamp,
      ms_to_wait: ms_to_wait
    })
  end

  @spec trace_log_pull_circuit_breaker_reset(integer()) :: :ok
  def trace_log_pull_circuit_breaker_reset(timestamp) do
    Telemetry.execute([:bedrock, :storage, :log_pull_circuit_breaker_reset], %{}, %{
      timestamp: timestamp
    })
  end
end
