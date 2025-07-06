defmodule Bedrock.ControlPlane.Director.Recovery.ServiceCollectionPhase do
  @moduledoc """
  Handles the :define_required_services phase of recovery.

  This phase is responsible for collecting references to all
  required services that will be part of the transaction system.
  """

  @doc """
  Execute the service collection phase of recovery.

  Collects all required service references from logs and storage teams
  for the transaction system layout.
  """
  @spec execute(map()) :: map()
  def execute(%{state: :define_required_services} = recovery_attempt) do
    required_service_ids =
      Enum.concat(
        recovery_attempt.logs |> Map.keys(),
        recovery_attempt.storage_teams |> Enum.flat_map(& &1.storage_ids)
      )
      |> Enum.uniq()

    required_services =
      recovery_attempt.available_services
      |> Map.take(required_service_ids)

    recovery_attempt
    |> Map.put(:required_services, required_services)
    |> Map.put(:state, :final_checks)
  end
end
