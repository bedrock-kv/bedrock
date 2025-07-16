defmodule RecoveryTestSupport do
  @moduledoc """
  Shared test utilities and fixtures for recovery tests.
  """

  # Mock cluster module for testing
  defmodule TestCluster do
    def name(), do: "test_cluster"
    def otp_name(component), do: :"test_#{component}"
  end

  @doc """
  Sets up logger metadata for recovery tests.
  This provides the basic metadata needed for tracing functions to work.

  For tests that need to capture telemetry events, use TestTelemetryHandler instead.
  """
  def setup_recovery_metadata(cluster \\ TestCluster, epoch \\ 1) do
    # Set up logger metadata using the provided cluster
    Logger.metadata(cluster: cluster, epoch: epoch)

    # Return cleanup function
    ExUnit.Callbacks.on_exit(fn ->
      Logger.metadata([])
    end)

    :ok
  end

  @doc """
  Creates a basic test context with node tracking.
  """
  def create_test_context() do
    node_tracking = :ets.new(:test_node_tracking, [:ordered_set])
    :ets.insert(node_tracking, [{Node.self(), :unknown, [:log, :storage], :up, true, nil}])
    %{node_tracking: node_tracking}
  end
end
