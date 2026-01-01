defmodule Bedrock.ControlPlane.Director.Recovery.TracingTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.Tracing
  alias Bedrock.DataPlane.Version

  # Mock cluster for testing
  defmodule TestCluster do
    @moduledoc false
    def name, do: "test_cluster"
  end

  setup do
    # Set up logger metadata for tracing functions
    Logger.metadata(cluster: TestCluster, epoch: 42)

    on_exit(fn ->
      Logger.metadata([])
    end)
  end

  # Helper function to capture trace logs
  defp capture_trace(event, data \\ %{}, metadata \\ %{}, opts \\ []) do
    capture_log(opts, fn ->
      Tracing.trace(event, data, metadata)
    end)
  end

  describe "trace/3" do
    test "traces recovery started event" do
      log_output = capture_trace(:started, %{}, %{cluster: TestCluster, epoch: 42, attempt: 1})
      assert log_output =~ "Bedrock [test_cluster/42]: Recovery attempt #1 started"
    end

    test "traces recovery completed event" do
      log_output = capture_trace(:completed, %{}, %{elapsed: {:millisecond, 1500}})
      assert log_output =~ "Bedrock [test_cluster/42]: Recovery completed in 1s!"
    end

    test "traces recovery stalled event" do
      log_output = capture_trace(:stalled, %{}, %{elapsed: {:second, 5}, reason: :timeout}, level: :error)
      assert log_output =~ "Bedrock [test_cluster/42]: Recovery stalled after 5s: :timeout"
    end

    test "traces services locked event" do
      log_output = capture_trace(:services_locked, %{n_services: 10, n_reporting: 8})
      assert log_output =~ "Bedrock [test_cluster/42]: Services 8/10 reporting"
    end

    test "traces first time initialization event" do
      log_output = capture_trace(:first_time_initialization)
      assert log_output =~ "Bedrock [test_cluster/42]: Initializing a brand new system"
    end

    test "traces creating vacancies events" do
      test_cases = [
        {0, 0, "No vacancies to create"},
        {3, 0, "Creating 3 log vacancies"},
        {0, 2, "Creating 2 storage team vacancies"},
        {3, 2, "Creating 3 log vacancies and 2 storage team vacancies"}
      ]

      for {log_count, storage_count, expected_message} <- test_cases do
        log_output =
          capture_trace(
            :creating_vacancies,
            %{n_log_vacancies: log_count, n_storage_team_vacancies: storage_count}
          )

        assert log_output =~ "Bedrock [test_cluster/42]: #{expected_message}"
      end
    end

    test "traces durable version chosen events" do
      test_cases = [
        {Version.from_integer(100), "<0,0,0,0,0,0,0,100>"},
        {nil, "nil"},
        {Version.from_integer(999_999_999), "<0,0,0,0,59,154,201,255>"},
        {Version.zero(), "<0,0,0,0,0,0,0,0>"}
      ]

      for {version, expected_output} <- test_cases do
        log_output = capture_trace(:durable_version_chosen, %{}, %{durable_version: version})
        assert log_output =~ "Bedrock [test_cluster/42]: Durable version chosen: #{expected_output}"
      end
    end

    test "traces team health events" do
      test_cases = [
        {[], [], "No teams available"},
        {["team_a", "team_b"], [], "All teams healthy (team_a, team_b)"},
        {[], ["team_c", "team_d"], "All teams degraded (team_c, team_d)"},
        {["team_a"], ["team_b"], "Healthy teams are team_a, with some teams degraded (team_b)"}
      ]

      for {healthy_teams, degraded_teams, expected_message} <- test_cases do
        log_output =
          capture_trace(:team_health, %{}, %{
            healthy_teams: healthy_teams,
            degraded_teams: degraded_teams
          })

        assert log_output =~ "Bedrock [test_cluster/42]: #{expected_message}"
      end
    end

    test "traces all log vacancies filled" do
      log_output = capture_trace(:all_log_vacancies_filled)
      assert log_output =~ "Bedrock [test_cluster/42]: All log vacancies filled"
    end

    test "traces all storage team vacancies filled" do
      log_output = capture_trace(:all_storage_team_vacancies_filled)
      assert log_output =~ "Bedrock [test_cluster/42]: All storage team vacancies filled"
    end

    test "traces replaying old logs events" do
      # Test with no logs to replay
      log_output =
        capture_trace(:replaying_old_logs, %{}, %{
          old_log_ids: [],
          new_log_ids: ["log:1", "log:2"],
          version_vector: {10, 50}
        })

      assert log_output =~ "Bedrock [test_cluster/42]: Version vector chosen: {10, 50}"
      assert log_output =~ "Bedrock [test_cluster/42]: No logs to replay"

      # Test with logs to replay
      log_output =
        capture_trace(:replaying_old_logs, %{}, %{
          old_log_ids: ["log:10", "log:20"],
          new_log_ids: ["log:1", "log:2"],
          version_vector: {10, 50}
        })

      assert log_output =~ "Bedrock [test_cluster/42]: Version vector chosen: {10, 50}"
      assert log_output =~ "Bedrock [test_cluster/42]: Replaying logs: {log:10, log:20} -> {log:1, log:2}"
    end

    test "traces suitable logs chosen" do
      log_output =
        capture_trace(:suitable_logs_chosen, %{}, %{
          suitable_logs: ["log:1", "log:2"],
          log_version_vector: {5, 25}
        })

      assert log_output =~ ~s(Bedrock [test_cluster/42]: Suitable logs chosen for copying: "log:1", "log:2")
      assert log_output =~ "Bedrock [test_cluster/42]: Version vector: {5, 25}"
    end

    test "traces storage unlocking" do
      log_output = capture_trace(:storage_unlocking, %{}, %{storage_worker_id: "storage_123"})
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
      assert :ok = Tracing.start()

      # Starting again should return error
      assert {:error, :already_exists} = Tracing.start()

      # Stop should work
      assert :ok = Tracing.stop()

      # Stopping again should return error
      assert {:error, :not_found} = Tracing.stop()

      # Should be able to start again after stop
      assert :ok = Tracing.start()
      assert :ok = Tracing.stop()
    end
  end
end
