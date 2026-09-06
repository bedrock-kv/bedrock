defmodule Bedrock.DataPlane.Log.Shale.Locking do
  @moduledoc false

  alias Bedrock.DataPlane.Log.Shale.DemuxControl
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.Service.RecoveryAuthority
  alias Bedrock.Service.RecoveryControl

  @spec lock_for_recovery(
          t :: State.t(),
          authority :: RecoveryAuthority.input()
        ) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists}
  def lock_for_recovery(t, authority) do
    with {:ok, authority} <- RecoveryAuthority.new(authority),
         :ok <- admit(authority, t.recovery_authority) do
      install(t, authority)
    end
  end

  defp admit(_incoming, nil), do: :ok

  defp admit(incoming, current) do
    case RecoveryAuthority.compare(incoming, current) do
      :older -> {:error, :newer_epoch_exists}
      :equal_generation_foreign -> {:error, :not_lock_owner}
      _ -> :ok
    end
  end

  defp install(t, authority) do
    if not is_nil(t.recovery_authority) and RecoveryAuthority.compare(authority, t.recovery_authority) == :same and
         t.mode == :locked and t.recovery_control.phase == :locked and is_nil(t.replay_operation) do
      {:ok, t}
    else
      record = RecoveryControl.locked(t.recovery_control, authority)

      case RecoveryControl.write(t.path, record) do
        :ok ->
          cancel_replay(t.replay_operation, :not_lock_owner)
          reject_pending(t.pending_pushes)
          # Quiesce the flush pipeline: a locked log must not write chunks. An
          # in-flight or retrying flush from this (now old-epoch) tree completing
          # after the new epoch starts would be wasted work at best; tearing the
          # tree down synchronously removes the question. The floor resets with
          # it and re-derives from fresh confirmations after recovery.
          DemuxControl.teardown(t.demux)

          {:ok,
           %{
             t
             | mode: :locked,
               epoch: authority.generation,
               director: nil,
               recovery_authority: RecoveryAuthority.external(authority),
               recovery_control: record,
               demux: nil,
               min_durable_version: nil,
               pending_pushes: %{},
               replay_operation: nil
           }}

        {:error, {:post_publish_sync_failed, reason}} ->
          exit({:recovery_authority_durability_uncertain, reason})

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @spec cancel_replay(nil | map(), term()) :: :ok
  def cancel_replay(nil, _reason), do: :ok

  def cancel_replay(%{waiters: waiters} = operation, reason) do
    if operation[:pid] && Process.alive?(operation.pid), do: Process.exit(operation.pid, :kill)

    if operation[:monitor] do
      receive do
        {:DOWN, ref, :process, _pid, _reason} when ref == operation.monitor -> :ok
      end
    end

    if operation[:guardian_monitor] do
      receive do
        {:DOWN, ref, :process, _pid, _reason} when ref == operation.guardian_monitor -> :ok
      end
    end

    if operation[:owner_monitor], do: Process.demonitor(operation.owner_monitor, [:flush])
    Enum.each(waiters, &GenServer.reply(&1, {:error, reason}))
    :ok
  end

  defp reject_pending(pending) do
    Enum.each(pending, fn
      {_key, %{waiters: waiters}} -> Enum.each(waiters, &GenServer.reply(&1, {:error, :not_lock_owner}))
      {_key, {_transaction, from}} -> GenServer.reply(from, {:error, :not_lock_owner})
    end)
  end
end
