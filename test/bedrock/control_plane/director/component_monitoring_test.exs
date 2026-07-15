defmodule Bedrock.ControlPlane.Director.ComponentMonitoringTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Server
  alias Bedrock.ControlPlane.Director.State

  describe "component failure handling" do
    test "terminates with shutdown reason when component fails" do
      # Spawn a test director process
      test_process = self()

      director_pid =
        spawn(fn ->
          # Simulate director receiving :DOWN message
          send(test_process, {:director_started, self()})

          receive do
            {:simulate_component_failure, failed_pid, reason} ->
              # This should cause the director to exit
              send(self(), {:DOWN, make_ref(), :process, failed_pid, reason})

              # Use the actual handle_info logic
              case Server.handle_info({:DOWN, make_ref(), :process, failed_pid, reason}, %{}) do
                {:stop, exit_reason, _state} ->
                  send(test_process, {:director_exited, exit_reason})
                  exit(exit_reason)

                other ->
                  send(test_process, {:unexpected_result, other})
                  exit(:unexpected_result)
              end
          end
        end)

      # Wait for director to start
      assert_receive {:director_started, ^director_pid}

      # Monitor the director
      monitor_ref = Process.monitor(director_pid)

      # Simulate component failure
      failed_component_pid = spawn(fn -> :ok end)
      failure_reason = :test_failure
      expected_shutdown_reason = {:shutdown, {:component_failure, failed_component_pid, failure_reason}}

      send(director_pid, {:simulate_component_failure, failed_component_pid, failure_reason})

      # Director should exit immediately with proper shutdown reason.
      # Generous budget: the default 100ms flakes under full-suite load.
      assert_receive {:director_exited, ^expected_shutdown_reason}, 1_000
      assert_receive {:DOWN, ^monitor_ref, :process, ^director_pid, ^expected_shutdown_reason}, 1_000
    end
  end

  describe "metadata materializer failure" do
    test "a :DOWN for the metadata materializer stops the director with component_failure" do
      materializer = spawn(fn -> :ok end)

      state = %State{
        state: :running,
        cluster: __MODULE__,
        epoch: 7,
        transaction_system_layout: %{metadata_materializer: materializer}
      }

      log =
        capture_log(fn ->
          assert {:stop, {:shutdown, {:component_failure, ^materializer, :killed}}, stopped} =
                   Server.handle_info({:DOWN, make_ref(), :process, materializer, :killed}, state)

          assert stopped.state == :stopped
        end)

      # The log line distinguishes metadata-materializer failure from a
      # generic component failure.
      assert log =~ "Metadata materializer"
    end

    test "a metadata materializer :DOWN emits a distinguishing telemetry event" do
      test_pid = self()
      handler_id = {__MODULE__, :metadata_materializer_failure}

      :telemetry.attach(
        handler_id,
        [:bedrock, :director, :metadata_materializer_failure],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      materializer = spawn(fn -> :ok end)

      state = %State{
        state: :running,
        cluster: __MODULE__,
        epoch: 7,
        transaction_system_layout: %{metadata_materializer: materializer}
      }

      capture_log(fn ->
        assert {:stop, {:shutdown, {:component_failure, ^materializer, :killed}}, _} =
                 Server.handle_info({:DOWN, make_ref(), :process, materializer, :killed}, state)
      end)

      assert_receive {:telemetry, [:bedrock, :director, :metadata_materializer_failure], _measurements,
                      %{epoch: 7, pid: ^materializer, reason: :killed}}
    end
  end
end
