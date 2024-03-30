defmodule Bedrock.ControlPlane.Config.Policies do
  defstruct [
    # Should nodes that volunteer to join the cluster be allowed to do so?
    allow_volunteer_nodes_to_join: true
  ]
end
