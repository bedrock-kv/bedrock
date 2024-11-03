defmodule Bedrock.DataPlane.Log.Limestone.Locking do
  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Log.Limestone.State

  @spec lock_for_recovery(State.t(), Director.ref(), Bedrock.epoch()) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists | String.t()}
  def lock_for_recovery(t, director, epoch) do
    State.transition_to(t, :locked, fn
      t when not is_nil(t.epoch) and epoch < t.epoch ->
        {:halt, :newer_epoch_exists}

      t ->
        %{t | epoch: epoch, director: director}
    end)
  end
end
