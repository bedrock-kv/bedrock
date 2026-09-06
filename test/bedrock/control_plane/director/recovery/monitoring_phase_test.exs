defmodule Bedrock.ControlPlane.Director.Recovery.MonitoringPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.MonitoringPhase
  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase

  # Helper to create test process that sleeps briefly
  defp test_process, do: spawn(fn -> :timer.sleep(100) end)

  # Helper to build a recovery attempt with monitored components. The
  # TSL carries no membership map; log pids come from the attempt's
  # transaction_services.
  defp attempt_with(opts \\ []) do
    logs = Keyword.get(opts, :logs, %{{:log, 1} => %{}})

    transaction_services =
      Keyword.get(
        opts,
        :transaction_services,
        Map.new(logs, fn {log_id, _} -> {log_id, %{kind: :log, status: {:up, test_process()}}} end)
      )

    recovery_attempt()
    |> Map.put(:sequencer, Keyword.get(opts, :sequencer, test_process()))
    |> Map.put(:proxies, Keyword.get(opts, :proxies, [test_process()]))
    |> Map.put(:resolvers, Keyword.get(opts, :resolvers, [{"start_key", test_process()}]))
    |> Map.put(:logs, logs)
    |> Map.put(:transaction_services, transaction_services)
  end

  # Helper to execute monitoring and verify next phase
  defp execute_and_verify(recovery_attempt, opts) do
    assert {_result, PersistencePhase} = MonitoringPhase.execute(recovery_attempt, opts)
  end

  describe "execute/1" do
    test "monitors all components when all PIDs are valid" do
      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      execute_and_verify(attempt_with(), %{monitor_fn: monitor_fn})

      # Should have monitored sequencer, 1 proxy, 1 resolver, 1 log (4 total)
      for _ <- 1..4, do: assert_received({:monitored, _})
    end

    test "records the refs on the attempt so an abandoned attempt can release them" do
      monitor_fn = fn _pid -> make_ref() end

      assert {result, PersistencePhase} = MonitoringPhase.execute(attempt_with(), %{monitor_fn: monitor_fn})

      # Sequencer, one proxy, one resolver, one log.
      assert length(result.component_monitors) == 4
      assert Enum.all?(result.component_monitors, &is_reference/1)
    end

    test "uses default Process.monitor when no monitor_fn provided" do
      execute_and_verify(attempt_with(), %{})
    end

    test "monitors multiple components correctly" do
      recovery_attempt =
        attempt_with(
          proxies: [test_process(), test_process()],
          resolvers: [{"key1", test_process()}, {"key2", test_process()}],
          logs: %{{:log, 1} => %{}, {:log, 2} => %{}}
        )

      execute_and_verify(recovery_attempt, %{})
    end

    test "monitors only the layout's logs — materializers are not epoch-fatal" do
      storage_pid = test_process()

      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt =
        attempt_with(
          logs: %{{:log, 1} => %{}},
          transaction_services: %{
            {:log, 1} => %{kind: :log, status: {:up, test_process()}},
            {:materializer, 1} => %{kind: :materializer, status: {:up, storage_pid}}
          }
        )

      execute_and_verify(recovery_attempt, %{monitor_fn: monitor_fn})

      # Should not receive monitoring message for storage PID
      refute_received {:monitored, ^storage_pid}
    end

    test "crashes if a layout log is not up — an epoch that cannot watch its logs must not run" do
      down_log_pid = test_process()

      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt =
        attempt_with(
          logs: %{{:log, 1} => %{}, {:log, 2} => %{}},
          transaction_services: %{
            {:log, 1} => %{kind: :log, status: {:up, test_process()}},
            {:log, 2} => %{kind: :log, status: {:down, down_log_pid}}
          }
        )

      assert_raise MatchError, fn ->
        MonitoringPhase.execute(recovery_attempt, %{monitor_fn: monitor_fn})
      end
    end
  end
end
