defmodule Bedrock.ControlPlane.Config.Policies do
  @moduledoc """
  A `Policies` is a data structure that describes the policies that are used
  to configure the cluster.
  """

  @typedoc """
  Struct representing the policies that are used to configure the cluster.

  ## Fields:
  - `allow_volunteer_nodes_to_join` - Should nodes that volunteer to join the cluster be allowed to do so?
  """
  @type t :: %{
          allow_volunteer_nodes_to_join: boolean()
        }

  @spec default_policies() :: t()
  def default_policies, do: %{allow_volunteer_nodes_to_join: true}
end
