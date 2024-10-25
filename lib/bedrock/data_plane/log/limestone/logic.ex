defmodule Bedrock.DataPlane.Log.Limestone.Logic do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Limestone.State
  alias Bedrock.Service.LogController

  @spec report_health_to_transaction_log_controller(State.t(), Log.health()) :: :ok
  def report_health_to_transaction_log_controller(t, health),
    do: :ok = LogController.report_health(t.controller, t.id, health)
end
