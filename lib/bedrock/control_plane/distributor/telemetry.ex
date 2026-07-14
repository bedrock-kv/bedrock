defmodule Bedrock.ControlPlane.Distributor.Telemetry do
  @moduledoc """
  Telemetry utilities for distributor lifecycle events.
  """

  @spec emit_distributor_started(module(), Bedrock.epoch(), pid()) :: :ok
  def emit_distributor_started(cluster, epoch, director) do
    :telemetry.execute(
      [:bedrock, :distributor, :started],
      %{},
      %{cluster: cluster, epoch: epoch, director: director}
    )
  end

  @spec emit_distributor_stopped(module(), Bedrock.epoch(), reason :: term()) :: :ok
  def emit_distributor_stopped(cluster, epoch, reason) do
    :telemetry.execute(
      [:bedrock, :distributor, :stopped],
      %{},
      %{cluster: cluster, epoch: epoch, reason: reason}
    )
  end
end
