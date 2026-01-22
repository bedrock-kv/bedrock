defmodule Bedrock.ControlPlane.Director.Recovery.InitializationPhase do
  @moduledoc """
  Creates the initial transaction system layout for a new cluster.

  Runs only when no logs existed in the previous layout. Creates log vacancy placeholders
  for the desired number of logs and the default two-shard layout. Uses vacancy placeholders
  instead of assigning specific services immediately.

  Log vacancies are assigned initial version ranges [0, 1] to establish the starting point
  for transaction processing. The shard layout divides the keyspace with a fundamental
  user/system boundary - one shard handles user data (empty string to 0xFF), the other
  handles system data (0xFF to end-of-keyspace).

  Creating vacancies rather than immediate service assignment allows later phases
  to optimize placement across available nodes and handle assignment failures
  independently.

  Always succeeds since it only creates in-memory structures. Transitions to
  log recruitment to begin service assignment.

  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [end_of_keyspace: 0, key_range: 2]
  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]
  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.DataPlane.Version

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    trace_recovery_first_time_initialization()

    log_vacancies = Enum.map(1..context.cluster_config.parameters.desired_logs, &{:vacancy, &1})

    # Default shard layout: user shard (tag 1) and system shard (tag 0)
    shard_layout = %{
      <<0xFF>> => {1, <<>>},
      end_of_keyspace() => {0, <<0xFF>>}
    }

    key_ranges = [
      {0, key_range(<<0xFF>>, end_of_keyspace())},
      {1, key_range(<<>>, <<0xFF>>)}
    ]

    resolver_descriptors =
      key_ranges
      |> Enum.with_index(1)
      |> Enum.map(fn {{_tag, {start_key, _end_key}}, index} ->
        resolver_descriptor(start_key, {:vacancy, index})
      end)

    log_tags = Enum.map(key_ranges, &elem(&1, 0))
    logs = Map.new(log_vacancies, &{&1, log_tags})

    updated_recovery_attempt =
      recovery_attempt
      |> Map.put(:durable_version, Version.zero())
      |> Map.put(:old_log_ids_to_copy, [])
      |> Map.put(:version_vector, {Version.zero(), Version.zero()})
      |> Map.put(:logs, logs)
      |> Map.put(:shard_layout, shard_layout)
      |> Map.put(:resolvers, resolver_descriptors)

    {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase}
  end
end
