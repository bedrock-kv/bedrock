defmodule Bedrock.ControlPlane.Director.Recovery.MonitoringPhaseTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.MonitoringPhase

  describe "execute/1" do
    test "monitors all components and logs debug messages" do
      sequencer_pid = spawn(fn -> :timer.sleep(100) end)
      proxy_pid = spawn(fn -> :timer.sleep(100) end)
      resolver_pid = spawn(fn -> :timer.sleep(100) end)
      log_pid = spawn(fn -> :timer.sleep(100) end)

      recovery_attempt = %{
        state: :monitor_components,
        sequencer: sequencer_pid,
        proxies: [proxy_pid],
        resolvers: [resolver_pid],
        required_services: %{
          {:log, 1} => %{kind: :log, status: {:up, log_pid}}
        }
      }

      log_output =
        capture_log([level: :debug], fn ->
          result = MonitoringPhase.execute(recovery_attempt)
          assert result.state == :completed
        end)

      assert log_output =~ "Director monitoring sequencer:"
      assert log_output =~ "Director monitoring commit proxy:"
      assert log_output =~ "Director monitoring resolver:"
      assert log_output =~ "Director monitoring log:"
    end

    test "handles empty components gracefully" do
      recovery_attempt = %{
        state: :monitor_components,
        sequencer: nil,
        proxies: [],
        resolvers: [],
        required_services: %{}
      }

      log_output =
        capture_log([level: :debug], fn ->
          result = MonitoringPhase.execute(recovery_attempt)
          assert result.state == :completed
        end)

      refute log_output =~ "Director monitoring sequencer:"
      refute log_output =~ "Director monitoring commit proxy:"
      refute log_output =~ "Director monitoring resolver:"
      refute log_output =~ "Director monitoring log:"
    end

    test "logs monitoring summary for multiple components" do
      sequencer_pid = spawn(fn -> :timer.sleep(100) end)
      proxy1_pid = spawn(fn -> :timer.sleep(100) end)
      proxy2_pid = spawn(fn -> :timer.sleep(100) end)
      resolver1_pid = spawn(fn -> :timer.sleep(100) end)
      resolver2_pid = spawn(fn -> :timer.sleep(100) end)
      log1_pid = spawn(fn -> :timer.sleep(100) end)
      log2_pid = spawn(fn -> :timer.sleep(100) end)

      recovery_attempt = %{
        state: :monitor_components,
        sequencer: sequencer_pid,
        proxies: [proxy1_pid, proxy2_pid],
        resolvers: [resolver1_pid, resolver2_pid],
        required_services: %{
          {:log, 1} => %{kind: :log, status: {:up, log1_pid}},
          {:log, 2} => %{kind: :log, status: {:up, log2_pid}},
          {:storage, 1} => %{kind: :storage, status: {:up, spawn(fn -> :timer.sleep(100) end)}}
        }
      }

      log_output =
        capture_log([level: :debug], fn ->
          result = MonitoringPhase.execute(recovery_attempt)
          assert result.state == :completed
        end)

      assert log_output =~ "Director monitoring 2 proxies, 2 resolvers, 2 logs, and 1 sequencer"
    end

    test "filters invalid PIDs during monitoring" do
      valid_pid = spawn(fn -> :timer.sleep(100) end)

      recovery_attempt = %{
        state: :monitor_components,
        sequencer: valid_pid,
        proxies: [valid_pid, :not_a_pid],
        resolvers: [:not_a_pid, valid_pid],
        required_services: %{
          {:log, 1} => %{kind: :log, status: {:up, valid_pid}},
          {:log, 2} => %{kind: :log, status: {:down, nil}},
          {:other, 1} => %{kind: :other, status: {:up, valid_pid}}
        }
      }

      log_output =
        capture_log([level: :debug], fn ->
          result = MonitoringPhase.execute(recovery_attempt)
          assert result.state == :completed
        end)

      monitoring_calls =
        String.split(log_output, "Director monitoring") |> length() |> Kernel.-(1)

      assert monitoring_calls >= 3
    end
  end
end
