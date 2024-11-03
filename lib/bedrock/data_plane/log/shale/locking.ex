defmodule Bedrock.DataPlane.Log.Shale.Locking do
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.ControlPlane.Director

  @spec lock_for_recovery(
          t :: State.t(),
          epoch :: Bedrock.epoch(),
          director :: Director.ref()
        ) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists}
  def lock_for_recovery(t, epoch, _director)
      when not is_nil(t.epoch) and epoch < t.epoch,
      do: {:error, :newer_epoch_exists}

  def lock_for_recovery(t, epoch, director) do
    {:ok, %{t | mode: :locked, epoch: epoch, director: director}}
  end
end
