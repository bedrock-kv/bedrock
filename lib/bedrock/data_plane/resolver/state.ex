defmodule Bedrock.DataPlane.Resolver.State do
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Tree

  @type t :: %__MODULE__{
          tree: Tree.t(),
          last_version: Bedrock.version(),
          waiting: %{
            Bedrock.version() => {Bedrock.version(), [Resolver.transaction()], GenServer.from()}
          }
        }
  defstruct tree: nil,
            last_version: nil,
            waiting: %{}
end
