defmodule Bedrock.ControlPlane.Director.Recovery.TracingTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.Tracing
  alias Bedrock.DataPlane.Version

  # Mock cluster for testing
  defmodule TestCluster do
    def name, do: "test_cluster"
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
          Tracing.trace(:durable_version_chosen, %{}, %{
            durable_version: Version.from_integer(100)
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Durable version chosen: <0,0,0,0,0,0,0,100>"
    end

    test "traces durable version chosen event with nil version" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:durable_version_chosen, %{}, %{
            durable_version: nil
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Durable version chosen: nil"
    end

    test "traces durable version chosen event with large version" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:durable_version_chosen, %{}, %{
            durable_version: Version.from_integer(999_999_999)
          })
        end)

      assert log_output =~
               "Bedrock [test_cluster/42]: Durable version chosen: <0,0,0,0,59,154,201,255>"
    end

    test "traces durable version chosen event with zero version" do
      log_output =
        capture_log(fn ->
          Tracing.trace(:durable_version_chosen, %{}, %{
            durable_version: Version.zero()
          })
        end)

      assert log_output =~ "Bedrock [test_cluster/42]: Durable version chosen: <0,0,0,0,0,0,0,0>"
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
