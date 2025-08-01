ExUnit.start()
Faker.start()

# Removed global telemetry handler - tests should set up their own handlers as needed

# Default test cluster for telemetry
defmodule DefaultTestCluster do
  def name, do: "test_cluster"
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

defmodule QuietDetsHandler do
  def init(_) do
    {:ok, []}
  end

  def handle_event({:error_report, _gl, {_pid, :dets, _report}}, state) do
    # Silently ignore DETS error reports
    {:ok, state}
  end

  def handle_event(event, state) do
    # Let other events through to default handler
    :error_logger_tty_h.handle_event(event, state)
  end

  # Other required callbacks...
  def handle_call(_request, state), do: {:ok, :ok, state}
  def handle_info(_info, state), do: {:ok, state}
  def terminate(_reason, _state), do: :ok
end

# Add the handler
:error_logger.add_report_handler(QuietDetsHandler, [])
