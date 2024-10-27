defmodule Bedrock.DataPlane.Log.Limestone.Logic do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Limestone.State
  alias Bedrock.Service.Foreman

  @spec report_health_to_foreman(State.t(), Log.health()) :: :ok
  def report_health_to_foreman(t, health),
    do: :ok = Foreman.report_health(t.foreman, t.id, health)
end
