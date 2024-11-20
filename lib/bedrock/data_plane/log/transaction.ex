defmodule Bedrock.DataPlane.Log.Transaction do
  @type t :: {Bedrock.version(), %{Bedrock.key() => Bedrock.value()}}
end
