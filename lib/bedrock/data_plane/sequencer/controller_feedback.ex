defmodule Bedrock.DataPlane.Sequencer.ControllerFeedback do
  alias Bedrock.DataPlane.Sequencer.State

  import Bedrock.DataPlane.Sequencer.Versions,
    only: [
      encode: 3
    ]

  @spec accept_invitation(State.t()) :: State.t()
  def accept_invitation(t) do
    GenServer.cast(
      t.controller,
      {:recruitment_invitation_accepted, self(), encode(t.epoch, 0, 0)}
    )

    t
  end

  @spec decline_invitation(State.t()) :: State.t()
  def decline_invitation(t) do
    GenServer.cast(
      t.controller,
      {:recruitment_invitation_declined, self()}
    )

    t
  end
end
