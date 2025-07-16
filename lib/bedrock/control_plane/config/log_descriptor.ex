defmodule Bedrock.ControlPlane.Config.LogDescriptor do
  @moduledoc """
  A `LogDescriptor` is a data structure that describes a log service within the
  system.
  """

  @type vacancy :: {:vacancy, tag :: Bedrock.range_tag()}
  @type t :: [Bedrock.range_tag()]
end
