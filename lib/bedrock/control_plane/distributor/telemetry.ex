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

  @spec emit_coverage_demand(module(), Bedrock.range_tag()) :: :ok
  def emit_coverage_demand(cluster, tag) do
    :telemetry.execute(
      [:bedrock, :distributor, :coverage_demand],
      %{},
      %{cluster: cluster, tag: tag}
    )
  end

  @spec emit_placeholder_parked(module(), Bedrock.range_tag()) :: :ok
  def emit_placeholder_parked(cluster, tag) do
    :telemetry.execute(
      [:bedrock, :distributor, :placeholder, :parked],
      %{count: 1},
      %{cluster: cluster, tag: tag}
    )
  end

  @spec emit_placeholder_forwarded(module(), Bedrock.range_tag()) :: :ok
  def emit_placeholder_forwarded(cluster, tag) do
    :telemetry.execute(
      [:bedrock, :distributor, :placeholder, :forwarded],
      %{count: 1},
      %{cluster: cluster, tag: tag}
    )
  end

  @spec emit_placeholder_drained(module(), Bedrock.range_tag(), count :: non_neg_integer()) :: :ok
  def emit_placeholder_drained(cluster, tag, count) do
    :telemetry.execute(
      [:bedrock, :distributor, :placeholder, :drained],
      %{count: count},
      %{cluster: cluster, tag: tag}
    )
  end

  @spec emit_placeholder_shed(module(), Bedrock.range_tag() | nil, count :: non_neg_integer(), reason :: term()) :: :ok
  def emit_placeholder_shed(cluster, tag, count, reason) do
    :telemetry.execute(
      [:bedrock, :distributor, :placeholder, :shed],
      %{count: count},
      %{cluster: cluster, tag: tag, reason: reason}
    )
  end
end
