defmodule Bedrock.ControlPlane.Director.Recovery.DataDistributionPhase do
  @moduledoc """
  Handles the :repair_data_distribution phase of recovery.

  This phase is responsible for repairing data distribution across
  the cluster and creating resolver descriptors from storage teams.
  """

  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]

  @doc """
  Execute the data distribution phase of recovery.

  Creates resolver descriptors from storage team key ranges and transitions
  to the sequencer definition phase.
  """
  @impl true
  def execute(%{state: :repair_data_distribution} = recovery_attempt, _context) do
    resolver_descriptors = create_resolver_descriptors_from_storage_teams(recovery_attempt.storage_teams)

    recovery_attempt
    |> Map.put(:resolvers, resolver_descriptors)
    |> Map.put(:state, :define_sequencer)
  end

  @spec create_resolver_descriptors_from_storage_teams([map()]) :: [ResolverDescriptor.t()]
  defp create_resolver_descriptors_from_storage_teams(storage_teams) do
    storage_teams
    |> Enum.map(fn storage_team ->
      {start_key, _end_key} = storage_team.key_range
      resolver_descriptor(start_key, nil)
    end)
    |> Enum.uniq_by(& &1.start_key)
    |> Enum.sort_by(& &1.start_key)
  end
end
