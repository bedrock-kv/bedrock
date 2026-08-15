defmodule Bedrock.DataPlane.Log.Shale.Locking do
  @moduledoc false

  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Log.Shale.DemuxControl
  alias Bedrock.DataPlane.Log.Shale.State

  @spec lock_for_recovery(
          t :: State.t(),
          epoch :: Bedrock.epoch(),
          director :: Director.ref()
        ) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists}
  def lock_for_recovery(t, epoch, _director) when not is_nil(t.epoch) and epoch < t.epoch,
    do: {:error, :newer_epoch_exists}

  def lock_for_recovery(t, epoch, director) do
    # Quiesce the flush pipeline: a locked log must not write chunks. An
    # in-flight or retrying flush from this (now old-epoch) tree completing
    # after the new epoch starts would be wasted work at best; tearing the
    # tree down synchronously removes the question. The floor resets with
    # it and re-derives from fresh confirmations after recovery.
    DemuxControl.teardown(t.demux)

    {:ok, %{t | mode: :locked, epoch: epoch, director: director, demux: nil, min_durable_version: nil}}
  end
end
