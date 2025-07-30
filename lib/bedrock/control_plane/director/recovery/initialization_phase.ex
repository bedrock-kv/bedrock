defmodule Bedrock.ControlPlane.Director.Recovery.InitializationPhase do
  @moduledoc """
  Creates the initial transaction system layout for a new cluster.

  Runs only when no previous cluster state exists. Creates log descriptors with
  evenly distributed key ranges and storage teams with the configured replication
  factor. Uses vacancy placeholders instead of assigning specific services immediately.

  The keyspace is divided evenly among the desired number of logs. Each log covers
  a contiguous key range from empty key to \\xff\\xff. Storage teams are created
  empty and filled by later recruitment phases.

  Creating vacancies rather than immediate service assignment allows later phases
  to optimize placement across available nodes and handle assignment failures
  independently.

  Always succeeds since it only creates in-memory structures. Transitions to
  log recruitment to begin service assignment.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [key_range: 2]
  import Bedrock.ControlPlane.Config.StorageTeamDescriptor, only: [storage_team_descriptor: 3]

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    trace_recovery_first_time_initialization()

    log_vacancies =
      1..context.cluster_config.parameters.desired_logs |> Enum.map(&{:vacancy, &1})

    storage_team_vacancies =
      1..context.cluster_config.parameters.desired_replication_factor |> Enum.map(&{:vacancy, &1})

    updated_recovery_attempt =
      recovery_attempt
      |> Map.put(:durable_version, 0)
      |> Map.put(:old_log_ids_to_copy, [])
      |> Map.put(:version_vector, {0, 0})
      |> Map.put(:logs, log_vacancies |> Map.new(&{&1, [0, 1]}))
      |> Map.put(:storage_teams, [
        storage_team_descriptor(
          0,
          key_range(<<0xFF>>, :end),
          storage_team_vacancies
        ),
        storage_team_descriptor(
          1,
          key_range(<<>>, <<0xFF>>),
          storage_team_vacancies
        )
      ])

    {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase}
  end
end
