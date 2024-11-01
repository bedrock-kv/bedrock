defmodule Bedrock.DataPlane.Log.Shale.Locking do
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.ControlPlane.ClusterController

  @spec lock_for_recovery(
          t :: State.t(),
          epoch :: Bedrock.epoch(),
          controller :: ClusterController.ref()
        ) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists}
  def lock_for_recovery(t, epoch, _controller) when epoch < t.epoch and not is_nil(t.epoch),
    do: {:error, :newer_epoch_exists}

  def lock_for_recovery(t, epoch, controller) do
    {:ok, %{t | mode: :locked, epoch: epoch, controller: controller}}
  end
end
