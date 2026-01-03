defmodule Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhase do
  @moduledoc """
  Creates vacancy placeholders that specify what services the new system needs without
  committing to specific service assignments.

  This phase solves a fundamental planning problem: how to specify system requirements
  (logs, storage servers, and resolvers for each shard) while allowing later phases to 
  make optimal placement decisions based on the complete picture of available resources.

  **Log Planning**: Creates a clean-slate log layout by generating the desired number 
  of log vacancies for each shard combination from the old system. This ensures the 
  new layout meets current configuration requirements rather than being constrained 
  by historical decisions.

  **Storage Planning**: Takes a conservative approach, calculating additional storage
  capacity needed per shard to meet replication requirements. Existing storage servers
  are preserved since they contain persistent data that's expensive to move.

  **Resolver Planning**: Creates resolver descriptors mapping storage team key ranges
  to resolver vacancies. These descriptors enable resolvers to route transactions to
  appropriate storage servers during conflict detection.

  Vacancies use numbered placeholders `{:vacancy, counter}` that represent "job postings"
  for services. The separation of planning from assignment allows recruitment phases to
  optimize service placement across available machines for fault tolerance and efficiency.

  Always succeeds since it only modifies in-memory planning structures. Transitions to
  VersionDeterminationPhase to establish the recovery baseline.

  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]
  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage

  @impl true
  def execute(recovery_attempt, context) do
    with {:ok, logs, n_log_vacancies} <-
           create_vacancies_for_logs(
             context.old_transaction_system_layout.logs,
             context.cluster_config.parameters.desired_logs
           ),
         {:ok, storage_teams, n_storage_team_vacancies} <-
           create_vacancies_for_storage_teams(
             context.old_transaction_system_layout.storage_teams,
             context.cluster_config.parameters.desired_replication_factor
           ) do
      trace_recovery_creating_vacancies(n_log_vacancies, n_storage_team_vacancies)

      # Create resolver descriptors from storage teams with vacancy marks
      resolver_descriptors = create_resolver_descriptors_from_storage_teams(storage_teams)

      updated_recovery_attempt =
        recovery_attempt
        |> Map.put(:logs, logs)
        |> Map.put(:storage_teams, storage_teams)
        |> Map.put(:resolvers, resolver_descriptors)

      {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhase}
    end
  end

  @spec create_vacancies_for_logs(%{Log.id() => LogDescriptor.t()}, pos_integer()) ::
          {:ok, %{Log.id() => LogDescriptor.t()}, non_neg_integer()}
  def create_vacancies_for_logs(logs, desired_logs) do
    {updated_logs, _} =
      logs
      |> Enum.group_by(&Enum.sort(elem(&1, 1)), &elem(&1, 0))
      |> Enum.reduce({%{}, 1}, fn {tags, _ids}, {acc_map, vacancy_counter} ->
        new_vacancies = Map.new(vacancy_counter..(vacancy_counter + desired_logs - 1), &{{:vacancy, &1}, tags})

        {Map.merge(acc_map, new_vacancies), vacancy_counter + desired_logs}
      end)

    {:ok, updated_logs, map_size(updated_logs)}
  end

  @type tag_set_roster ::
          %{[Bedrock.range_tag()] => [Storage.id()]}
  @type expanded_tag_set_roster ::
          %{[Bedrock.range_tag()] => [Storage.id() | StorageTeamDescriptor.vacancy()]}

  @spec create_vacancies_for_storage_teams(
          [StorageTeamDescriptor.t()],
          pos_integer()
        ) :: {:ok, [StorageTeamDescriptor.t()], non_neg_integer()}
  def create_vacancies_for_storage_teams(storage_teams, desired_replication) do
    rosters_by_tag_set = tag_set_rosters_from_storage_teams(storage_teams)

    case expand_rosters_and_add_vacancies(rosters_by_tag_set, desired_replication) do
      {_expanded_rosters_by_tag_set, 0} ->
        {:ok, storage_teams, 0}

      {expanded_rosters_by_tag_set, n_storage_team_vacancies} ->
        {:ok, apply_expanded_roster_to_storage_teams(storage_teams, expanded_rosters_by_tag_set),
         n_storage_team_vacancies}
    end
  end

  @spec tag_set_rosters_from_storage_teams([StorageTeamDescriptor.t()]) ::
          %{[tag :: Bedrock.range_tag()] => [Storage.id()]}
  def tag_set_rosters_from_storage_teams(storage_teams) do
    storage_teams
    |> Enum.flat_map(fn storage_team ->
      Enum.map(storage_team.storage_ids, &{&1, storage_team.tag})
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.group_by(&Enum.sort(elem(&1, 1)), &elem(&1, 0))
  end

  @spec expand_rosters_and_add_vacancies(tag_set_roster(), pos_integer()) ::
          {expanded_tag_set_roster(), non_neg_integer()}
  def expand_rosters_and_add_vacancies(rosters_by_tag_set, desired_replication) do
    rosters_by_tag_set
    |> Enum.reduce(
      {[], 0},
      fn {tags, storage_ids}, {expanded_sets, vacancy_count} ->
        n_vacancies = desired_replication - Enum.count(storage_ids)

        if n_vacancies > 0 do
          new_vacancy_count = vacancy_count + n_vacancies - 1
          new_vacancies = Enum.map(vacancy_count..new_vacancy_count, &{:vacancy, &1})
          {[{tags, new_vacancies ++ storage_ids} | expanded_sets], new_vacancy_count}
        else
          {[{tags, storage_ids} | expanded_sets], vacancy_count}
        end
      end
    )
    |> then(fn {expanded_sets, vacancy_count} ->
      {Map.new(expanded_sets), vacancy_count}
    end)
  end

  @spec apply_expanded_roster_to_storage_teams([StorageTeamDescriptor.t()], tag_set_roster()) ::
          [StorageTeamDescriptor.t()]
  def apply_expanded_roster_to_storage_teams(storage_teams, expanded_rosters_by_tag_set) do
    new_rosters_by_tag =
      expanded_rosters_by_tag_set
      |> Enum.flat_map(fn {tags, storage_ids} ->
        Enum.map(tags, fn tag ->
          {tag, Enum.sort(storage_ids)}
        end)
      end)
      |> Map.new()

    Enum.map(
      storage_teams,
      &Map.put(&1, :storage_ids, Map.get(new_rosters_by_tag, &1.tag))
    )
  end

  @spec create_resolver_descriptors_from_storage_teams([map()]) :: [ResolverDescriptor.t()]
  defp create_resolver_descriptors_from_storage_teams(storage_teams) do
    storage_teams
    |> Enum.filter(&Map.has_key?(&1, :key_range))
    |> Enum.with_index()
    |> Enum.map(fn {storage_team, index} ->
      {start_key, _end_key} = storage_team.key_range
      resolver_descriptor(start_key, {:vacancy, index + 1})
    end)
    |> Enum.uniq_by(& &1.start_key)
    |> Enum.sort_by(& &1.start_key)
  end
end
