defmodule Bedrock.ControlPlane.Config.LogDescriptor do
  @moduledoc """
  A `LogDescriptor` is a data structure that describes a log service within the
  system.
  """

  @type vacancy :: {:vacancy, tag :: term()}
  @type t :: [Bedrock.range_tag()]
end
