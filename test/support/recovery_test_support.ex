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
  Creates a basic test context with node tracking and old transaction system layout.
  """
  def create_test_context(opts \\ []) do
    node_tracking = :ets.new(:test_node_tracking, [:ordered_set])
    :ets.insert(node_tracking, [{Node.self(), :unknown, [:log, :storage], :up, true, nil}])

    old_transaction_system_layout =
      Keyword.get(opts, :old_transaction_system_layout, %{
        logs: %{},
        storage_teams: []
      })

    %{
      node_tracking: node_tracking,
      old_transaction_system_layout: old_transaction_system_layout,
      cluster_config: %{
        coordinators: [],
        parameters: %{
          desired_logs: 2,
          desired_replication_factor: 3,
          desired_commit_proxies: 1,
          desired_coordinators: 1,
          desired_read_version_proxies: 1,
          ping_rate_in_hz: 10,
          retransmission_rate_in_hz: 5,
          transaction_window_in_ms: 1000
        }
      },
      available_services: %{},
      lock_token: "test_token",
      coordinator: self()
    }
  end
end
