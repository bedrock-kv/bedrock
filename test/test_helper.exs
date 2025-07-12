ExUnit.start()
Faker.start()

# Start recovery telemetry tracing for all tests
unless Process.whereis(:bedrock_trace_director_recovery) do
  Bedrock.ControlPlane.Director.Recovery.Tracing.start()
end

Mox.defmock(Bedrock.Raft.MockInterface, for: Bedrock.Raft.Interface)

# Define behavior for Resolver testing
defmodule Bedrock.DataPlane.Resolver.Behaviour do
  @callback resolve_transactions(
              ref :: any(),
              last_version :: Bedrock.version(),
              commit_version :: Bedrock.version(),
              [Bedrock.DataPlane.Resolver.transaction()],
              keyword()
            ) ::
              {:ok, aborted :: [index :: integer()]}
              | {:error, :timeout}
              | {:error, :unavailable}
end

Mox.defmock(Bedrock.DataPlane.ResolverMock, for: Bedrock.DataPlane.Resolver.Behaviour)
