defmodule Bedrock.DataPlane.Sequencer.DirectorFeedback do
  alias Bedrock.DataPlane.Sequencer.State

  @spec accept_invitation(State.t()) :: State.t()
  def accept_invitation(t) do
    GenServer.cast(
      t.director,
      {:recruitment_invitation, :accepted, self(), t.last_committed_version}
    )

    t
  end

  @spec decline_invitation(State.t(), director :: pid()) :: State.t()
  def decline_invitation(t, director) do
    GenServer.cast(
      director,
      {:recruitment_invitation, :declined, self()}
    )

    t
  end
end
