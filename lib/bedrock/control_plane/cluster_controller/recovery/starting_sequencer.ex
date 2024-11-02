defmodule Bedrock.ControlPlane.ClusterController.Recovery.StartingSequencer do
  alias Bedrock.DataPlane.Sequencer

  def start_sequencer(
        controller,
        epoch,
        {_first_version, last_committed_version},
        start_supervised
      ) do
    with {:ok, sequencer} <-
           start_supervised.(
             Sequencer.child_spec(
               controller: controller,
               epoch: epoch,
               last_committed_version: last_committed_version
             ),
             Node.self()
           ) do
      {:ok, sequencer}
    else
      {:error, reason} -> {:error, {:failed_to_start_sequencer, reason}}
    end
  end
end
