defmodule Bedrock.ControlPlane.ClusterController.Recovery do
  @moduledoc false

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  import Bedrock.Internal.Time, only: [now: 0]

  import Bedrock.ControlPlane.Config.Changes,
    only: [
      set_epoch: 2,
      update_recovery_attempt: 2,
      set_transaction_system_layout: 2,
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

  @spec claim_config(State.t()) :: State.t()
  def claim_config(t) do
    t |> update_config(&set_epoch(&1, t.epoch))
  end

  @spec start_new_recovery_attempt(State.t()) :: State.t()
  def start_new_recovery_attempt(t) do
    t
    |> update_config(fn config ->
      config
      |> update_recovery_attempt(
        &RecoveryAttempt.new(
          &1,
          t.epoch,
          now(),
          :recruiting,
          config.transaction_system_layout
        )
      )
      |> set_transaction_system_layout(
        TransactionSystemLayout.new()
        |> set_controller(self())
      )
    end)
  end

  @spec recover(State.t()) :: State.t()
  def recover(t) do
    with {:ok, services, info_by_id} <-
           try_to_lock_services_for_recovery(t.config.services, t.epoch, 5_000),
         {:ok, log_info, storage_info} <- organize_available_service_info(info_by_id),
         {:ok, replication_factor} <- fetch_replication_factor(t.config),
         {:ok, newest_durable_version} <-
           determine_newest_durable_version(
             storage_info,
             replication_factor |> determine_quorum()
           ),
         {:ok, desired_logs} <- fetch_desired_logs(t.config),
         {:ok, suitable_logs, version_vector} <-
           log_info
           |> extract_version_vectors_by_id()
           |> determine_suitable_logs_for_recovery(desired_logs |> determine_quorum()) do
      t
      |> update_config(fn config ->
        config
        |> update_transaction_system_layout(
          &TransactionSystemLayout.Tools.set_services(&1, services)
        )
        |> update_recovery_attempt(&RecoveryAttempt.Mutations.update_state(&1, :locked))
      end)
    else
      {:error, :newer_epoch_exists} ->
        t
        |> update_config(fn config ->
          config
          |> update_recovery_attempt(&RecoveryAttempt.Mutations.update_state(&1, :failed))
        end)
    end
  end

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
    end
  end

  @spec extract_version_vectors_by_id(%{Log.id() => map()}) ::
          [{Log.id(), Bedrock.version_vector()}]
  defp extract_version_vectors_by_id(log_info) do
    log_info |> Enum.map(fn {id, info} -> {id, {info[:oldest_tx_id], info[:last_tx_id]}} end)
  end

  defp fetch_replication_factor(config) do
    get_in(config.parameters.replication_factor)
    |> case do
      nil -> {:error, :unable_to_determine_replication_factor}
      factor -> {:ok, factor}
    end
  end

  defp fetch_desired_logs(config) do
    get_in(config.parameters.desired_logs)
    |> case do
      nil -> {:error, :unable_to_determine_desired_logs}
      factor -> {:ok, factor}
    end
  end

  def determine_newest_durable_version(storage_info_by_id, quorum) do
    storage_info_by_id
    |> Enum.map(fn {_, info} -> info[:durable_version] end)
    |> Enum.reject(&is_nil/1)
    |> Enum.sort()
    |> Enum.at(-quorum)
    |> case do
      nil -> {:error, :insufficient_storage}
      version -> {:ok, version}
    end
  end

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
          {:ok, [ServiceDescriptor.t()], recovery_info_by_pid :: keyword()}
          | {:error, :newer_epoch_exists}
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
    |> Enum.reduce_while({:ok, [], %{}}, fn
      {_, {:error_newer_epoch_exists} = error}, _ ->
        {:halt, error}

      {%{id: id} = service, {:ok, pid, info}}, {:ok, services, info_by_id} ->
        {:cont,
         {:ok, [service |> ServiceDescriptor.up(pid) | services], Map.put(info_by_id, id, info)}}

      {service, {:error, _}}, {:ok, services, info_by_id} ->
        {:cont, {:ok, [service |> ServiceDescriptor.down() | services], info_by_id}}

      {:exit, {service, _}}, {:ok, services, info_by_id} ->
        {:cont, {:ok, [service |> ServiceDescriptor.down() | services], info_by_id}}
    end)
  end

  @spec try_to_lock_service_for_recovery(ServiceDescriptor.t(), Bedrock.epoch()) ::
          {:ok, pid(), Bedrock.version_vector()} | {:error, :unavailable}
  def try_to_lock_service_for_recovery(%{kind: :log, last_seen: name}, epoch),
    do: Log.lock_for_recovery(name, self(), epoch)

  def try_to_lock_service_for_recovery(%{kind: :storage, last_seen: name}, epoch),
    do: Storage.lock_for_recovery(name, self(), epoch)

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
