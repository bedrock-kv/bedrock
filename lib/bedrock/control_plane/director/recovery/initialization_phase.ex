defmodule Bedrock.ControlPlane.Director.Recovery.InitializationPhase do
  @moduledoc """
  Handles the :first_time_initialization phase of recovery.

  This phase is responsible for setting up a brand new cluster with empty logs
  and storage teams by creating placeholders based on desired configuration.

  See: [Recovery Guide](docs/knowledge_base/01-guides/recovery-guide.md#recovery-process)
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  import Bedrock, only: [key_range: 2]
  import Bedrock.ControlPlane.Config.StorageTeamDescriptor, only: [storage_team_descriptor: 3]

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the first-time initialization phase of recovery.

  Creates initial log and storage team configurations for a new cluster
  and transitions to log vacancy recruitment.
  """
  @impl true
  def execute(%RecoveryAttempt{state: :first_time_initialization} = recovery_attempt, _context) do
    trace_recovery_first_time_initialization()

    log_vacancies =
      1..recovery_attempt.parameters.desired_logs |> Enum.map(&{:vacancy, &1})

    storage_team_vacancies =
      1..recovery_attempt.parameters.desired_replication_factor |> Enum.map(&{:vacancy, &1})

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
    |> Map.put(:state, :recruit_logs_to_fill_vacancies)
  end
end
