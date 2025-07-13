defmodule Bedrock.DataPlane.Resolver.State do
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Tree

  @type mode :: :locked | :running

  @type t :: %__MODULE__{
          tree: Tree.t(),
          oldest_version: Bedrock.version(),
          last_version: Bedrock.version(),
          waiting: %{
            Bedrock.version() =>
              {Bedrock.version(), [Resolver.transaction_summary()], GenServer.from()}
          },
          mode: mode(),
          lock_token: binary()
        }
  defstruct tree: nil,
            oldest_version: nil,
            last_version: nil,
            waiting: %{},
            mode: :locked,
            lock_token: nil
end
