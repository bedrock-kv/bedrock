minio_available? =
  try do
    ExUnit.CaptureIO.capture_io(fn ->
      {:ok, _} = Bedrock.Test.Minio.start_link()
      :ok = Bedrock.Test.Minio.wait_for_ready()
    end)

    true
  rescue
    RuntimeError -> false
    MatchError -> false
  end

Application.put_env(:bedrock, :minio_available, minio_available?)
System.put_env("BEDROCK_MINIO_AVAILABLE", if(minio_available?, do: "1", else: "0"))

distributed_requested? =
  Enum.any?(System.argv(), fn arg ->
    arg == "distributed" or String.starts_with?(arg, "distributed:")
  end)

distributed_enabled? =
  System.get_env("BEDROCK_INCLUDE_DISTRIBUTED") in ["1", "true", "TRUE"] or distributed_requested?

excludes =
  []
  |> then(fn excludes ->
    if minio_available?, do: excludes, else: [:s3 | excludes]
  end)
  |> then(fn excludes ->
    if distributed_enabled?, do: excludes, else: [:distributed | excludes]
  end)

if !minio_available? do
  IO.puts("MinIO not available - tests tagged :s3 will be skipped")
end

if !distributed_enabled? do
  IO.puts("Distributed tests disabled - set BEDROCK_INCLUDE_DISTRIBUTED=1 or pass --include distributed")
end

ExUnit.start(exclude: excludes)
Faker.start()

# Removed global telemetry handler - tests should set up their own handlers as needed

# Default test cluster for telemetry
defmodule DefaultTestCluster do
  @moduledoc false
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
Mox.defmock(MockRepo, for: Bedrock.Repo)
Mox.stub(MockRepo, :transact, fn callback -> callback.() end)

# Define behavior for Resolver testing
defmodule Bedrock.DataPlane.Resolver.Behaviour do
  @moduledoc false
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
  @moduledoc false
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
