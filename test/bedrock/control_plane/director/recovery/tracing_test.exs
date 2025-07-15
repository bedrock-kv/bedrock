defmodule Bedrock.ControlPlane.Director.Recovery.TracingTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.Tracing

  # Mock cluster for testing
  defmodule TestCluster do
    def name(), do: "test_cluster"
  end

  setup do
    # Set up logger metadata for tracing functions
    Logger.metadata(cluster: TestCluster, epoch: 42)

    on_exit(fn ->
      Logger.metadata([])
    end)
  end

  describe "trace/3" do
    test "traces recovery started event" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:started, %{}, %{cluster: TestCluster, epoch: 42, attempt: 1})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Recovery attempt #1 started"
    end

    test "traces recovery completed event" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:completed, %{}, %{elapsed: {:millisecond, 1500}})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Recovery completed in 1s!"
    end

    test "traces recovery stalled event" do
      log_output =
        capture_log([level: :error], fn ->
          Tracing.trace(:stalled, %{}, %{elapsed: {:second, 5}, reason: :timeout})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Recovery stalled after 5s: :timeout"
    end

    test "traces services locked event" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:services_locked, %{n_services: 10, n_reporting: 8}, %{})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Services 8/10 reporting"
    end

    test "traces first time initialization event" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:first_time_initialization, %{}, %{})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Initializing a brand new system"
    end

    test "traces creating vacancies event with no vacancies" do
      log_output =
        capture_log(fn ->
          Tracing.trace(
            :creating_vacancies,
            %{n_log_vacancies: 0, n_storage_team_vacancies: 0},
            %{}
          )
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: No vacancies to create"
    end

    test "traces creating vacancies event with log vacancies only" do
      log_output =
        capture_log(fn ->
          Tracing.trace(
            :creating_vacancies,
            %{n_log_vacancies: 3, n_storage_team_vacancies: 0},
            %{}
          )
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Creating 3 log vacancies"
    end

    test "traces creating vacancies event with storage team vacancies only" do
      log_output =
        capture_log(fn ->
          Tracing.trace(
            :creating_vacancies,
            %{n_log_vacancies: 0, n_storage_team_vacancies: 2},
            %{}
          )
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Creating 2 storage team vacancies"
    end

    test "traces creating vacancies event with both types" do
      log_output =
        capture_log(fn ->
          Tracing.trace(
            :creating_vacancies,
            %{n_log_vacancies: 3, n_storage_team_vacancies: 2},
            %{}
          )
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Creating 3 log vacancies and 2 storage team vacancies"
    end

    test "traces durable version chosen event" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:durable_version_chosen, %{}, %{durable_version: 100})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Durable version chosen: 100"
    end

    test "traces team health with no teams" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:team_health, %{}, %{healthy_teams: [], degraded_teams: []})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: No teams available"
    end

    test "traces team health with all healthy teams" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:team_health, %{}, %{
            healthy_teams: ["team_a", "team_b"],
            degraded_teams: []
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: All teams healthy (team_a, team_b)"
    end

    test "traces team health with all degraded teams" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:team_health, %{}, %{
            healthy_teams: [],
            degraded_teams: ["team_c", "team_d"]
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: All teams degraded (team_c, team_d)"
    end

    test "traces team health with mixed teams" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:team_health, %{}, %{
            healthy_teams: ["team_a"],
            degraded_teams: ["team_b"]
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Healthy teams are team_a, with some teams degraded (team_b)"
    end

    test "traces all log vacancies filled" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:all_log_vacancies_filled, %{}, %{})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: All log vacancies filled"
    end

    test "traces all storage team vacancies filled" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:all_storage_team_vacancies_filled, %{}, %{})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: All storage team vacancies filled"
    end

    test "traces replaying old logs with no logs to replay" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:replaying_old_logs, %{}, %{
            old_log_ids: [],
            new_log_ids: ["log:1", "log:2"],
            version_vector: {10, 50}
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Version vector chosen: {10, 50}"
      assert log_output =~ "Bedrock [test_cluster/42]: No logs to replay"
    end

    test "traces replaying old logs with logs to replay" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:replaying_old_logs, %{}, %{
            old_log_ids: ["log:10", "log:20"],
            new_log_ids: ["log:1", "log:2"],
            version_vector: {10, 50}
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Version vector chosen: {10, 50}"

      assert log_output =~
               "Bedrock [test_cluster/42]: Replaying logs: {log:10, log:20} -> {log:1, log:2}"
    end

    test "traces suitable logs chosen" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:suitable_logs_chosen, %{}, %{
            suitable_logs: ["log:1", "log:2"],
            log_version_vector: {5, 25}
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Suitable logs chosen for copying: \"log:1\", \"log:2\""

      assert log_output =~ "Bedrock [test_cluster/42]: Version vector: {5, 25}"
    end

    test "traces storage unlocking" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:storage_unlocking, %{}, %{storage_worker_id: "storage_123"})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Storage worker storage_123 unlocking"
    end

    test "traces cleanup started with no workers" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:cleanup_started, %{}, %{total_obsolete_workers: 0, affected_nodes: []})
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: No obsolete workers to clean up"
    end

    test "traces cleanup started with workers on single node" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:cleanup_started, %{}, %{
            total_obsolete_workers: 5,
            affected_nodes: [:node1]
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Starting cleanup of 5 obsolete workers on 1 node"
    end

    test "traces cleanup started with workers on multiple nodes" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:cleanup_started, %{}, %{
            total_obsolete_workers: 10,
            affected_nodes: [:node1, :node2, :node3]
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Starting cleanup of 10 obsolete workers across 3 nodes"
    end

    test "traces cleanup completed with no workers" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:cleanup_completed, %{}, %{
            total_obsolete_workers: 5,
            successful_cleanups: 0,
            failed_cleanups: 0
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: No workers were cleaned up"
    end

    test "traces cleanup completed with all successful" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:cleanup_completed, %{}, %{
            total_obsolete_workers: 5,
            successful_cleanups: 5,
            failed_cleanups: 0
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Successfully cleaned up all 5 obsolete workers"
    end

    test "traces cleanup completed with partial success" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:cleanup_completed, %{}, %{
            total_obsolete_workers: 10,
            successful_cleanups: 7,
            failed_cleanups: 0
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Successfully cleaned up 7/10 workers"
    end

    test "traces cleanup completed with all failures" do
      log_output =
        capture_log([level: :error], fn ->
          Tracing.trace(:cleanup_completed, %{}, %{
            total_obsolete_workers: 5,
            successful_cleanups: 0,
            failed_cleanups: 5
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Failed to clean up all 5 workers"
    end

    test "traces cleanup completed with mixed results" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:cleanup_completed, %{}, %{
            total_obsolete_workers: 10,
            successful_cleanups: 6,
            failed_cleanups: 4
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Cleaned up 6/10 workers (4 failed)"
    end

    test "traces node cleanup started" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:node_cleanup_started, %{}, %{node: :node1, worker_count: 3})
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Starting cleanup of 3 workers on node node1"
    end

    test "traces node cleanup completed with no workers" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:node_cleanup_completed, %{}, %{
            node: :node1,
            successful_cleanups: 0,
            failed_cleanups: 0
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: No workers cleaned up on node node1"
    end

    test "traces node cleanup completed with all successful" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:node_cleanup_completed, %{}, %{
            node: :node1,
            successful_cleanups: 3,
            failed_cleanups: 0
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Successfully cleaned up 3 workers on node node1"
    end

    test "traces node cleanup completed with all failures" do
      log_output =
        capture_log([level: :error], fn ->
          Tracing.trace(:node_cleanup_completed, %{}, %{
            node: :node1,
            successful_cleanups: 0,
            failed_cleanups: 3
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Failed to clean up 3 workers on node node1"
    end

    test "traces node cleanup completed with mixed results" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:node_cleanup_completed, %{}, %{
            node: :node1,
            successful_cleanups: 2,
            failed_cleanups: 1
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Cleaned up 2 workers on node node1 (1 failed)"
    end
  end

  describe "handler/4" do
    test "calls trace with correct arguments" do
      measurements = %{duration: 1000}
      metadata = %{cluster: TestCluster, epoch: 42, attempt: 1}

      log_output =
        capture_log(fn ->
          Tracing.handler([:bedrock, :recovery, :started], measurements, metadata, nil)
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Recovery attempt #1 started"
    end
  end

  describe "start/0 and stop/0" do
    test "attaches and detaches telemetry handlers" do
      # Test that we can start without error
      assert Tracing.start() == :ok

      # Starting again should return error
      assert Tracing.start() == {:error, :already_exists}

      # Stop should work
      assert Tracing.stop() == :ok

      # Stopping again should return error  
      assert Tracing.stop() == {:error, :not_found}

      # Should be able to start again after stop
      assert Tracing.start() == :ok
      assert Tracing.stop() == :ok
    end
  end
end
