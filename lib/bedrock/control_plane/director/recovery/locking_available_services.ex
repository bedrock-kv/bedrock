defmodule Bedrock.ControlPlane.Director.Recovery.LockingAvailableServices do
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker

  @spec lock_available_services_timeout() :: Bedrock.timeout_in_ms()
  def lock_available_services_timeout, do: 200

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

    - `available_services` - A list of service descriptors that describe the
      services available.
    - `epoch` - The epoch within which to begin recovery.
    - `timeout_in_ms` - The maximum time in milliseconds to wait for
      services to respond to rejoin invitations.
  """
  @spec lock_available_services(
          %{Worker.id() => ServiceDescriptor.t()},
          Bedrock.quorum(),
          Bedrock.timeout_in_ms()
        ) ::
          {:ok, locked_ids :: MapSet.t(Worker.id()),
           updated_services :: %{Worker.id() => ServiceDescriptor.t()},
           new_log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()},
           new_storage_recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()}}
          | {:error, :newer_epoch_exists}
  def lock_available_services(available_services, epoch, timeout_in_ms) do
    available_services
    |> Task.async_stream(
      fn {id, service} ->
        {id, service, lock_service_for_recovery(service, epoch)}
      end,
      timeout: timeout_in_ms,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while({MapSet.new(), %{}, %{}}, fn
      {:ok, {_, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {id, service, {:ok, pid, info}}}, {locked_ids, services, info_by_id} ->
        {:cont,
         {MapSet.put(locked_ids, id), Map.put(services, id, up(service, pid)),
          Map.put(info_by_id, id, info)}}

      {:ok, {id, service, {:error, _}}}, {locked_ids, services, info_by_id} ->
        {:cont, {locked_ids, Map.put(services, id, down(service)), info_by_id}}
    end)
    |> case do
      {:error, _reason} = error ->
        error

      {locked_ids, updated_services, info_by_id} ->
        grouped_recovery_info =
          info_by_id
          |> Enum.group_by(&Map.get(elem(&1, 1), :kind))

        new_log_recovery_info_by_id =
          Map.new(Map.get(grouped_recovery_info, :log, []))

        new_storage_recovery_info_by_id =
          Map.new(Map.get(grouped_recovery_info, :storage, []))

        {:ok, locked_ids, updated_services, new_log_recovery_info_by_id,
         new_storage_recovery_info_by_id}
    end
  end

  def lock_service_for_recovery(%{kind: :log, last_seen: name}, epoch),
    do: Log.lock_for_recovery(name, epoch)

  def lock_service_for_recovery(%{kind: :storage, last_seen: name}, epoch),
    do: Storage.lock_for_recovery(name, epoch)

  def lock_service_for_recovery(_, _), do: {:error, :unavailable}

  defp up(service, pid), do: %{service | status: {:up, pid}}

  def down(service), do: %{service | status: :down}
end
