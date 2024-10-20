defmodule Bedrock.DataPlane.Sequencer.ControllerFeedback do
  alias Bedrock.DataPlane.Sequencer.State

  @spec accept_invitation(State.t()) :: State.t()
  def accept_invitation(t) do
    GenServer.cast(
      t.controller,
      {:recruitment_invitation, :accepted, self(), t.last_committed_version}
    )

    t
  end

  @spec decline_invitation(State.t(), controller :: pid()) :: State.t()
  def decline_invitation(t, controller) do
    GenServer.cast(
      controller,
      {:recruitment_invitation, :declined, self()}
    )

    t
  end
end
