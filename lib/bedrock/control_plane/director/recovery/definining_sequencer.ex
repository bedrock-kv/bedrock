defmodule Bedrock.ControlPlane.Director.Recovery.DefiningSequencer do
  alias Bedrock.DataPlane.Sequencer

  def define_sequencer(
        director,
        epoch,
        {_first_version, last_committed_version},
        start_supervised
      ) do
    node = Node.self()

    with {:ok, sequencer} <-
           start_supervised.(
             Sequencer.child_spec(
               director: director,
               epoch: epoch,
               last_committed_version: last_committed_version
             ),
             node
           ) do
      {:ok, sequencer}
    else
      {:error, reason} -> {:error, {:failed_to_start, :sequencer, node, reason}}
    end
  end
end
