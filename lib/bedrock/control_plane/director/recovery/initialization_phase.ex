defmodule Bedrock.ControlPlane.Director.Recovery.InitializationPhase do
  @moduledoc """
  Creates the initial transaction system layout for a new cluster.

  Runs only when no logs existed in the previous layout. Creates log vacancy placeholders 
  for the desired number of logs and exactly two storage teams with fixed key range 
  boundaries. Uses vacancy placeholders instead of assigning specific services immediately.

  Log vacancies are assigned initial version ranges [0, 1] to establish the starting point 
  for transaction processing. Storage teams divide the keyspace with a fundamental user/system 
  boundary - one team handles user data (empty string to 0xFF), the other handles system data 
  (0xFF to end-of-keyspace), with keys above the user space reserved for system metadata.

  Creating vacancies rather than immediate service assignment allows later phases
  to optimize placement across available nodes and handle assignment failures
  independently.

  Always succeeds since it only creates in-memory structures. Transitions to
  log recruitment to begin service assignment.

  See the New Cluster Initialization section in `docs/knowlege_base/02-deep/recovery-narrative.md` 
  for detailed explanation of the initialization flow and rationale.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [key_range: 2]
  import Bedrock.ControlPlane.Config.StorageTeamDescriptor, only: [storage_team_descriptor: 3]
  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.DataPlane.Version

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    trace_recovery_first_time_initialization()

    log_vacancies =
      1..context.cluster_config.parameters.desired_logs |> Enum.map(&{:vacancy, &1})

    storage_team_vacancies =
      1..context.cluster_config.parameters.desired_replication_factor |> Enum.map(&{:vacancy, &1})

    key_ranges = [
      {0, key_range(<<0xFF>>, :end)},
      {1, key_range(<<>>, <<0xFF>>)}
    ]

    storage_teams =
      key_ranges
      |> Enum.map(fn {tag, key_range} ->
        storage_team_descriptor(tag, key_range, storage_team_vacancies)
      end)

    resolver_descriptors =
      key_ranges
      |> Enum.with_index(1)
      |> Enum.map(fn {{_tag, {start_key, _end_key}}, index} ->
        resolver_descriptor(start_key, {:vacancy, index})
      end)

    updated_recovery_attempt =
      recovery_attempt
      |> Map.put(:durable_version, Version.zero())
      |> Map.put(:old_log_ids_to_copy, [])
      |> Map.put(:version_vector, {Version.zero(), Version.zero()})
      |> Map.put(
        :logs,
        log_vacancies |> Map.new(&{&1, [Version.zero(), Version.from_integer(1)]})
      )
      |> Map.put(:storage_teams, storage_teams)
      |> Map.put(:resolvers, resolver_descriptors)

    {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase}
  end
end
