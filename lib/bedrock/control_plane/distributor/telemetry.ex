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

  @spec emit_coverage_sweep(module(), Bedrock.epoch(), uncovered :: non_neg_integer()) :: :ok
  def emit_coverage_sweep(cluster, epoch, uncovered) do
    :telemetry.execute(
      [:bedrock, :distributor, :coverage_sweep],
      %{uncovered: uncovered},
      %{cluster: cluster, epoch: epoch}
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

  @spec emit_recruitment_started(module(), Bedrock.epoch(), Bedrock.range_tag()) :: :ok
  def emit_recruitment_started(cluster, epoch, tag) do
    :telemetry.execute(
      [:bedrock, :distributor, :recruitment, :started],
      %{},
      %{cluster: cluster, epoch: epoch, tag: tag}
    )
  end

  @spec emit_recruitment_succeeded(
          module(),
          Bedrock.epoch(),
          Bedrock.range_tag(),
          node(),
          duration_us :: non_neg_integer()
        ) :: :ok
  def emit_recruitment_succeeded(cluster, epoch, tag, node, duration_us) do
    :telemetry.execute(
      [:bedrock, :distributor, :recruitment, :succeeded],
      %{duration_us: duration_us},
      %{cluster: cluster, epoch: epoch, tag: tag, node: node}
    )
  end

  @spec emit_recruitment_failed(
          module(),
          Bedrock.epoch(),
          Bedrock.range_tag(),
          reason :: term(),
          duration_us :: non_neg_integer()
        ) :: :ok
  def emit_recruitment_failed(cluster, epoch, tag, reason, duration_us) do
    :telemetry.execute(
      [:bedrock, :distributor, :recruitment, :failed],
      %{duration_us: duration_us},
      %{cluster: cluster, epoch: epoch, tag: tag, reason: reason}
    )
  end

  @spec emit_readoption_started(module(), Bedrock.epoch(), Bedrock.range_tag(), node()) :: :ok
  def emit_readoption_started(cluster, epoch, tag, node) do
    :telemetry.execute(
      [:bedrock, :distributor, :readoption, :started],
      %{},
      %{cluster: cluster, epoch: epoch, tag: tag, node: node}
    )
  end

  @spec emit_readoption_succeeded(
          module(),
          Bedrock.epoch(),
          Bedrock.range_tag(),
          node(),
          duration_us :: non_neg_integer()
        ) :: :ok
  def emit_readoption_succeeded(cluster, epoch, tag, node, duration_us) do
    :telemetry.execute(
      [:bedrock, :distributor, :readoption, :succeeded],
      %{duration_us: duration_us},
      %{cluster: cluster, epoch: epoch, tag: tag, node: node}
    )
  end

  @spec emit_readoption_failed(
          module(),
          Bedrock.epoch(),
          Bedrock.range_tag(),
          node(),
          reason :: term(),
          duration_us :: non_neg_integer()
        ) :: :ok
  def emit_readoption_failed(cluster, epoch, tag, node, reason, duration_us) do
    :telemetry.execute(
      [:bedrock, :distributor, :readoption, :failed],
      %{duration_us: duration_us},
      %{cluster: cluster, epoch: epoch, tag: tag, node: node, reason: reason}
    )
  end

  @spec emit_materializer_down(module(), Bedrock.epoch(), Bedrock.range_tag(), reason :: term()) :: :ok
  def emit_materializer_down(cluster, epoch, tag, reason) do
    :telemetry.execute(
      [:bedrock, :distributor, :materializer_down],
      %{},
      %{cluster: cluster, epoch: epoch, tag: tag, reason: reason}
    )
  end

  @spec emit_idle_spindown(module(), Bedrock.epoch(), Bedrock.range_tag()) :: :ok
  def emit_idle_spindown(cluster, epoch, tag) do
    :telemetry.execute(
      [:bedrock, :distributor, :idle_spindown],
      %{},
      %{cluster: cluster, epoch: epoch, tag: tag}
    )
  end

  @spec emit_healing_started(module(), Bedrock.epoch(), Bedrock.range_tag()) :: :ok
  def emit_healing_started(cluster, epoch, tag) do
    :telemetry.execute(
      [:bedrock, :distributor, :healing, :started],
      %{},
      %{cluster: cluster, epoch: epoch, tag: tag}
    )
  end

  @spec emit_healing_completed(module(), Bedrock.epoch(), Bedrock.range_tag()) :: :ok
  def emit_healing_completed(cluster, epoch, tag) do
    :telemetry.execute(
      [:bedrock, :distributor, :healing, :completed],
      %{},
      %{cluster: cluster, epoch: epoch, tag: tag}
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
