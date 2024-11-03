defmodule Bedrock.Internal.Tracing.CoordinationTelemetry do
  require Logger

  defp handler_id, do: "bedrock_trace_coordination_telemetry"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :cluster, :coordination, :consensus_reached]
      ],
      &__MODULE__.log_event/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def log_event(
        [:bedrock, :cluster, :coordination, :consensus_reached],
        _measurements,
        %{cluster: cluster, transaction_id: transaction_id} = _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Reached consensus at transaction #{inspect(transaction_id)}.",
      ansi_color: :green
    )
  end
end
