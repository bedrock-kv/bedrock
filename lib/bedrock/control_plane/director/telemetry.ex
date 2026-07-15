defmodule Bedrock.ControlPlane.Director.Telemetry do
  @moduledoc false
  alias Bedrock.ControlPlane.Director
  alias Bedrock.Telemetry

  @doc """
  Emits a telemetry event indicating that the metadata materializer (system
  shard, tag 0) died. Its death is a core-component failure: the director
  stops and the coordinator restarts recovery.
  """
  @spec trace_metadata_materializer_failure(
          cluster :: module(),
          epoch :: Bedrock.epoch(),
          pid :: pid(),
          reason :: term()
        ) :: :ok
  def trace_metadata_materializer_failure(cluster, epoch, pid, reason) do
    Telemetry.execute([:bedrock, :director, :metadata_materializer_failure], %{}, %{
      cluster: cluster,
      epoch: epoch,
      pid: pid,
      reason: reason
    })
  end

  @doc """
  Emits a telemetry event indicating that the director applied a post-recovery
  delta to the transaction system layout's `shard_materializers` map.
  """
  @spec trace_tsl_delta_applied(
          cluster :: module(),
          epoch :: Bedrock.epoch(),
          delta :: Director.tsl_delta()
        ) :: :ok
  def trace_tsl_delta_applied(cluster, epoch, delta) do
    Telemetry.execute([:bedrock, :director, :tsl_delta_applied], %{shard_count: map_size(delta)}, %{
      cluster: cluster,
      epoch: epoch,
      delta: delta
    })
  end
end
