defmodule Bedrock.ControlPlane.ClusterController.Telemetry do
  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.Telemetry

  @doc """
  Emits a telemetry event indicating that the cluster controller has started
  the recovery process.
  """
  @spec emit_recovery_started(t :: State.t()) :: State.t()
  def emit_recovery_started(t) do
    Telemetry.emit([:bedrock, :cluster, :controller, :recovery, :started], %{}, %{
      cluster: t.cluster,
      controller: t.controller,
      at: t.config.recovery_attempt.at
    })

    t
  end
end
