defmodule Bedrock.ControlPlane.ClusterController.Recovery do
  @moduledoc false

  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage

  import Bedrock.Internal.Time, only: [now: 0]

  import Bedrock.ControlPlane.Config.Changes,
    only: [
      set_epoch: 2,
      set_recovery_attempt: 2,
      update_recovery_attempt: 2,
      update_transaction_system_layout: 2
    ]

  import Bedrock.ControlPlane.Config.TransactionSystemLayout.Tools,
    only: [
      set_controller: 2
    ]

  import Bedrock.ControlPlane.ClusterController.State.Changes,
    only: [
      update_config: 2
    ]

  import Bedrock.ControlPlane.ClusterController.Telemetry,
    only: [
      trace_recovery_attempt_started: 1,
      trace_recovery_services_locked: 3,
      trace_recovery_durable_version_chosen: 3,
      trace_recovery_suitable_logs_chosen: 3
    ]

  @spec claim_config(State.t()) :: State.t()
  def claim_config(t) do
    t
    |> update_config(fn config ->
      config
      |> set_epoch(t.epoch)
      |> update_transaction_system_layout(fn tsl ->
        tsl
        |> set_controller(self())
      end)
    end)
  end

  @spec start_new_recovery_attempt(State.t()) :: State.t()
  def start_new_recovery_attempt(t) when is_nil(t.config.recovery_attempt) do
    t
    |> update_config(fn config ->
      config
      |> set_recovery_attempt(
        RecoveryAttempt.new(
          t.epoch,
          now(),
          :recruiting,
          config.transaction_system_layout
        )
      )
      |> update_transaction_system_layout(
        &%{
          &1
          | sequencer: nil,
            rate_keeper: nil,
            data_distributor: nil,
            proxies: [],
            transaction_resolvers: []
        }
      )
    end)
    |> then(fn t ->
      :ok = trace_recovery_attempt_started(t)
      t
    end)
  end

  def start_new_recovery_attempt(t) do
    t
    |> update_config(fn config ->
      config
      |> update_recovery_attempt(
        &%{
          &1
          | attempt: &1.attempt + 1,
            started_at: now()
        }
      )
    end)
    |> then(fn t ->
      :ok = trace_recovery_attempt_started(t)
      t
    end)
  end

  @spec try_to_fix_stalled_recovery_if_needed(State.t()) :: State.t()
  def try_to_fix_stalled_recovery_if_needed(t) when t.config.recovery_attempt.state == :stalled,
    do: t |> recover()

  def try_to_fix_stalled_recovery_if_needed(t), do: t

  @spec recover(State.t()) :: State.t()
  def recover(t) do
    transaction_system_layout =
      get_in(t.config.transaction_system_layout)

    with {:ok, services, info_by_id} <-
           try_to_lock_services_for_recovery(
             transaction_system_layout.services,
             t.epoch,
             200
           ),
         :ok <-
           trace_recovery_services_locked(t, length(services), map_size(info_by_id)),
         {:ok, log_info, storage_info} <- organize_available_service_info(info_by_id),
         {:ok, replication_factor} <- fetch_replication_factor(t.config),
         {:ok, durable_version, degraded_teams} <-
           determine_durable_version(
             transaction_system_layout.storage_teams,
             storage_info,
             replication_factor |> determine_quorum()
           ),
         :ok <- trace_recovery_durable_version_chosen(t, durable_version, degraded_teams),
         {:ok, desired_logs} <- fetch_desired_logs(t.config),
         {:ok, suitable_logs,
          {oldest_version_in_logs, last_version_of_epoch} = log_version_vector} <-
           log_info
           |> extract_version_vectors_by_id()
           |> determine_suitable_logs_for_recovery(desired_logs |> determine_quorum()),
         :ok <- trace_recovery_suitable_logs_chosen(t, suitable_logs, log_version_vector) do
      IO.inspect({max(durable_version, oldest_version_in_logs), last_version_of_epoch})

      t
      |> update_config(fn config ->
        config
        |> update_transaction_system_layout(fn tsl ->
          tsl
          |> TransactionSystemLayout.Tools.set_services(services)
        end)
        |> update_recovery_attempt(&RecoveryAttempt.Mutations.set_state(&1, :recruiting))
      end)
    else
      {:error, _reason} ->
        t
        |> update_config(fn config ->
          config
          |> update_recovery_attempt(&RecoveryAttempt.Mutations.set_state(&1, :stalled))
        end)
    end
  end

  @spec organize_available_service_info(%{Bedrock.service_id() => map()}) ::
          {:ok, %{Log.id() => map()}, %{Storage.id() => map()}}
          | {:error, :no_log_info | :no_storage_info | :uninitialized}
  defp organize_available_service_info(info_by_id) do
    info_by_id
    |> Enum.group_by(&elem(&1, 1)[:kind])
    |> case do
      %{log: log_info, storage: storage_info} ->
        {:ok, log_info |> Map.new(), storage_info |> Map.new()}

      %{log: _} ->
        {:error, :no_storage_info}

      %{storage: _} ->
        {:error, :no_log_info}

      %{} ->
        {:error, :uninitialized}
    end
  end

  @spec extract_version_vectors_by_id(%{Log.id() => map()}) ::
          [{Log.id(), Bedrock.version_vector()}]
  defp extract_version_vectors_by_id(log_info) do
    log_info |> Enum.map(fn {id, info} -> {id, {info[:oldest_tx_id], info[:last_tx_id]}} end)
  end

  @spec fetch_replication_factor(Config.t()) ::
          {:ok, non_neg_integer()} | {:error, :unable_to_determine_replication_factor}
  defp fetch_replication_factor(config) do
    get_in(config.parameters.replication_factor)
    |> case do
      value when is_integer(value) -> {:ok, value}
      _ -> {:error, :unable_to_determine_replication_factor}
    end
  end

  @spec fetch_desired_logs(Config.t()) ::
          {:ok, non_neg_integer()} | {:error, :unable_to_determine_desired_logs}
  defp fetch_desired_logs(config) do
    get_in(config.parameters.desired_logs)
    |> case do
      value when is_integer(value) -> {:ok, value}
      nil -> {:error, :unable_to_determine_desired_logs}
    end
  end

  @doc """
  Determines the latest durable version for a given set of storage teams.

  For each storage team, the function checks if a quorum of storage nodes
  agree on a version that should be considered the latest durable version.
  If any storage team does not have enough supporting nodes to reach a
  quorum, it is classified as failed.

  ## Parameters

    - `teams`: A list of `StorageTeamDescriptor` structs, each representing a
      storage team whose durable version needs to be determined.

    - `info_by_id`: A map where each storage identifier is mapped to its
      `durable_version` and `oldest_version` information.

    - `quorum`: The minimum number of nodes that must agree on a durable
      version for consensus.

  ## Returns

    - `{:ok, durable_version, degraded_teams}`: On successful determination of
      the durable version, where `durable_version` is the latest version
      agreed upon, and `degraded_teams` lists teams not reaching the full
      healthy quorum.

    - `{:error, {:insufficient_storage, failed_tags}}`: If any storage team
      lacks sufficient storage servers to meet the quorum requirements. We
      return the full set of failed tags in this case.
  """
  @spec determine_durable_version(
          teams :: [StorageTeamDescriptor.t()],
          info_by_id :: %{
            Storage.id() => %{
              durable_version: Bedrock.version(),
              oldest_version: Bedrock.version()
            }
          },
          quorum :: non_neg_integer()
        ) ::
          {:ok, Bedrock.version(), degraded_teams :: [Bedrock.range_tag()]}
          | {:error, {:insufficient_storage, failed_tags :: [Bedrock.range_tag()]}}
  def determine_durable_version(teams, info_by_id, quorum) do
    Enum.zip(
      teams |> Enum.map(& &1.tag),
      teams
      |> Enum.map(&determine_durable_version_and_status_for_storage_team(&1, info_by_id, quorum))
    )
    |> Enum.reduce({nil, [], []}, fn
      {_tag, {:ok, version, :healthy}}, {min_version, degraded, failed} ->
        {min(version, min_version), degraded, failed}

      {tag, {:ok, version, :degraded}}, {min_version, degraded, failed} ->
        {min(version, min_version), [tag | degraded], failed}

      {tag, {:error, :insufficient_storage}}, {min_version, degraded, failed} ->
        {min_version, degraded, [tag | failed]}
    end)
    |> case do
      {_, _, [_at_least_one | _rest] = failed} -> {:error, {:insufficient_storage, failed}}
      {min_version, degraded, []} -> {:ok, min_version, degraded}
    end
  end

  @doc """
  Determine the most recent durable version available among a list of storage
  servers, based on the provided quorum. It's also important that we discard
  any storage servers from consideration that do not have a full-copy (back to
  the initial transaction) of the data. We also use the quorum to determine
  whether or not the team is healthy or degraded.

  ## Parameters

    - `team`: A `StorageTeamDescriptor` struct representing a storage
      teams involved in the operation.

    - `info_by_id`: A map where each key is a storage identifier and each value
      is a map (hopefully) containing the `:durable_version` and
      `:oldest_version`.

    - `quorum`: The minimum number of storage servers that must agree on the
      version to form a consensus.

  ## Returns

    - `{:ok, durable_version, status}`: The most recent durable version of the
      storage where consensus was reached and an indicator of the team's status,
      both based on the `quorum`.

    - `{:error, :insufficient_storage}`: Indicates that there aren't enough
      storage servers available to meet the quorum requirements for a consensus
      on the durable version.
  """
  @spec determine_durable_version_and_status_for_storage_team(
          team :: StorageTeamDescriptor.t(),
          info_by_id :: %{
            Storage.id() => %{
              durable_version: Bedrock.version(),
              oldest_durable_version: Bedrock.version()
            }
          },
          quorum :: non_neg_integer()
        ) ::
          {:ok, Bedrock.version(), status :: :healthy | :degraded}
          | {:error, :insufficient_storage}
  def determine_durable_version_and_status_for_storage_team(team, info_by_id, quorum) do
    durable_versions =
      team.storage_ids
      |> Enum.map(&Map.get(info_by_id, &1))
      |> Enum.reject(&is_nil/1)
      |> Enum.filter(&(Map.get(&1, :oldest_durable_version) == :initial))
      |> Enum.map(&Map.get(&1, :durable_version))

    durable_versions
    |> Enum.sort()
    |> Enum.at(-quorum)
    |> case do
      nil ->
        {:error, :insufficient_storage}

      version ->
        {:ok, version, durability_status_for_storage_team(length(durable_versions), quorum)}
    end
  end

  defp durability_status_for_storage_team(durable_versions, quorum)
       when durable_versions == quorum,
       do: :healthy

  defp durability_status_for_storage_team(_, _), do: :degraded

  @doc """
  Attempts to lock services for recovery. It then sends an invitation to each
  eligible service in parallel, requesting that they lock for recovery in this
  new epoch.

  The operation runs within the bounded-time  specified by `timeout_in_ms` and
  all of the services are contacted asynchronously. If the function encounters a
  service indicating that a newer epoch exists, it will halt further processing
  and return that as an error. Otherwise, it will collect the process IDs and
  lock/status information and return that in a success tuple.

  ## Parameters

    - `service_descriptors` - A list of service descriptors that describe
      the services available.
    - `epoch` - The epoch within which to begin recovery.
    - `timeout_in_ms` - The maximum time in milliseconds to wait for
      services to respond to rejoin invitations.

  ## Returns

    - `{:ok, pids_by_id, info_by_id}` - a map of service IDs to their current
      process IDs, and a map of service IDs to their recovery info.
    - `{:error, :newer_epoch_exists}` - If any service indicates that a newer
      epoch exists.
  """
  @spec try_to_lock_services_for_recovery(
          [ServiceDescriptor.t()],
          Bedrock.epoch(),
          Bedrock.timeout_in_ms()
        ) ::
          {:error, :newer_epoch_exists}
          | {:ok, [ServiceDescriptor.t()],
             recovery_info_by_pid :: %{Bedrock.service_id() => map()}}
  def try_to_lock_services_for_recovery(services, epoch, timeout_in_ms) do
    services
    |> Task.async_stream(
      fn service ->
        service
        |> try_to_lock_service_for_recovery(epoch)
        |> then(&{service, &1})
      end,
      timeout: timeout_in_ms,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while({[], %{}}, fn
      {:ok, {_, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {%{id: id} = service, {:ok, pid, info}}}, {services, info_by_id} ->
        {:cont,
         {[service |> ServiceDescriptor.up(pid) | services], Map.put(info_by_id, id, info)}}

      {:ok, {service, {:error, _}}}, {services, info_by_id} ->
        {:cont, {[service |> ServiceDescriptor.down() | services], info_by_id}}

      {:exit, {service, _}}, {services, info_by_id} ->
        {:cont, {[service |> ServiceDescriptor.down() | services], info_by_id}}
    end)
    |> case do
      {:error, _reason} = error -> error
      {services, info_by_id} -> {:ok, services, info_by_id}
    end
  end

  @spec try_to_lock_service_for_recovery(ServiceDescriptor.t(), Bedrock.epoch()) ::
          {:ok, pid(), recovery_info :: map()} | {:error, :unavailable}
  def try_to_lock_service_for_recovery(%{kind: :log, last_seen: name}, epoch),
    do: Log.lock_for_recovery(name, epoch)

  def try_to_lock_service_for_recovery(%{kind: :storage, last_seen: name}, epoch),
    do: Storage.lock_for_recovery(name, epoch)

  def try_to_lock_service_for_recovery(_, _), do: {:error, :unavailable}

  @spec determine_suitable_logs_for_recovery(
          [{Log.id(), Bedrock.version_vector()}],
          quorum :: non_neg_integer()
        ) :: {:ok, [Log.id()], Bedrock.version_vector()} | {:error, :unable_to_meet_quorum}
  def determine_suitable_logs_for_recovery(version_vectors_by_log_id, quorum) do
    version_vectors_by_log_id
    |> Enum.to_list()
    |> combinations(quorum)
    |> Enum.map(fn group ->
      oldest = group |> Enum.map(fn {_, {oldest, _}} -> oldest end) |> Enum.max()
      newest = group |> Enum.map(fn {_, {_, newest}} -> newest end) |> Enum.min()
      {group |> Enum.map(&elem(&1, 0)), {oldest, newest}}
    end)
    |> Enum.filter(&valid_range?/1)
    |> Enum.sort_by(fn {_, {oldest, newest}} -> newest - oldest end, :desc)
    |> List.first()
    |> case do
      nil ->
        {:error, :unable_to_meet_quorum}

      {log_ids, version_vector} ->
        {:ok, log_ids, version_vector}
    end
  end

  defp valid_range?({_, {oldest, newest}}), do: newest >= oldest

  @spec combinations([any()], non_neg_integer()) :: [[any()]]
  defp combinations(_list, 0), do: [[]]
  defp combinations([], _num), do: []

  defp combinations([head | tail], num),
    do: Enum.map(combinations(tail, num - 1), &[head | &1]) ++ combinations(tail, num)

  defp determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)
end
