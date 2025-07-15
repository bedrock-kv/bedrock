ExUnit.start()
Faker.start()

# Removed global telemetry handler - tests should set up their own handlers as needed

# Default test cluster for telemetry
defmodule DefaultTestCluster do
  def name(), do: "test_cluster"
  def otp_name(component), do: :"test_#{component}"

  @doc """
  Sets up logger metadata for telemetry handlers.
  Call this in test setup to ensure proper cluster identification.
  """
  def setup_telemetry_metadata(epoch \\ 1) do
    Logger.metadata(cluster: __MODULE__, epoch: epoch)
    :ok
  end
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
