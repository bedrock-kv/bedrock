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
  @type t :: %__MODULE__{
          allow_volunteer_nodes_to_join: boolean()
        }

  defstruct allow_volunteer_nodes_to_join: true

  @spec new() :: t()
  def new, do: %__MODULE__{}
end
