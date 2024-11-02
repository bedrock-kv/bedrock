defmodule Bedrock.DataPlane.Resolver.State do
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Tree

  @type t :: %__MODULE__{
          tree: Tree.t(),
          oldest_version: Bedrock.version(),
          last_version: Bedrock.version(),
          waiting: %{
            Bedrock.version() => {Bedrock.version(), [Resolver.transaction()], GenServer.from()}
          }
        }
  defstruct tree: nil,
            oldest_version: nil,
            last_version: nil,
            waiting: %{}
end
