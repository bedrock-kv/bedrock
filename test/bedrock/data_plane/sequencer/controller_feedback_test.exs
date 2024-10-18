defmodule Bedrock.DataPlane.Sequencer.ControllerFeedbackTest do
  use ExUnit.Case, async: true
  alias Bedrock.DataPlane.Sequencer.ControllerFeedback
  alias Bedrock.DataPlane.Sequencer.State

  import ControllerFeedback, only: [accept_invitation: 1, decline_invitation: 1]
  import Bedrock.DataPlane.Sequencer.Versions, only: [encode: 3]

  setup do
    state = %State{controller: self(), epoch: 1..1_000_000 |> Enum.random()}
    {:ok, state: state}
  end

  test "accept_invitation/1 sends recruitment_invitation_accepted message", %{state: state} do
    accept_invitation(state)

    expected_controller = self()
    expected_version = encode(state.epoch, 0, 0)

    assert_received {:"$gen_cast", {:recruitment_invitation_accepted, ^expected_controller, ^expected_version}}
  end

  test "decline_invitation/1 sends recruitment_invitation_declined message", %{state: state} do
    decline_invitation(state)

    expected_controller = self()
    assert_received {:"$gen_cast", {:recruitment_invitation_declined, ^expected_controller}}
  end
end
