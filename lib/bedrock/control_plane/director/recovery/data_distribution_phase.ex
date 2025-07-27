defmodule Bedrock.ControlPlane.Director.Recovery.DataDistributionPhase do
  @moduledoc """
  Repairs data distribution across the cluster and creates resolver descriptors.

  Analyzes storage teams to identify those that need data repair due to degraded
  replicas or missing data. Initiates repair processes to restore full replication
  across all teams.

  Creates resolver descriptors that map key ranges to storage teams. Resolvers
  use these descriptors to route read and write operations to the appropriate
  storage servers during transaction processing.

  The data distribution repair ensures that all storage teams have sufficient
  replicas to handle failures. Teams identified as degraded in earlier phases
  are prioritized for repair.

  Always succeeds since resolver descriptors can be created from available
  storage teams. Transitions to :define_sequencer to begin starting transaction
  system components.
  """

  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]

  @impl true
  def execute(%{state: :repair_data_distribution} = recovery_attempt, _context) do
    resolver_descriptors =
      create_resolver_descriptors_from_storage_teams(recovery_attempt.storage_teams)

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
