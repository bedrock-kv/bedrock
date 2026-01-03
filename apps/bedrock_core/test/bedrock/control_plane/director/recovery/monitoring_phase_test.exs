defmodule Bedrock.ControlPlane.Director.Recovery.MonitoringPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.MonitoringPhase
  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase

  # Helper to create test process that sleeps briefly
  defp test_process, do: spawn(fn -> :timer.sleep(100) end)

  # Helper to create test transaction system layout
  defp create_layout(opts \\ []) do
    sequencer = Keyword.get(opts, :sequencer, test_process())
    proxies = Keyword.get(opts, :proxies, [test_process()])
    resolvers = Keyword.get(opts, :resolvers, [{"start_key", test_process()}])
    logs = Keyword.get(opts, :logs, %{{:log, 1} => %{}})

    # Create services map based on the components
    services =
      Keyword.get(opts, :services, %{
        {:log, 1} => %{kind: :log, status: {:up, test_process()}}
      })

    %{
      sequencer: sequencer,
      proxies: proxies,
      resolvers: resolvers,
      logs: logs,
      services: services
    }
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

      recovery_attempt = Map.put(recovery_attempt(), :transaction_system_layout, create_layout())

      execute_and_verify(recovery_attempt, %{monitor_fn: monitor_fn})

      # Should have monitored sequencer, 1 proxy, 1 resolver, 1 log (4 total)
      for _ <- 1..4, do: assert_received({:monitored, _})
    end

    test "uses default Process.monitor when no monitor_fn provided" do
      recovery_attempt = Map.put(recovery_attempt(), :transaction_system_layout, create_layout())

      execute_and_verify(recovery_attempt, %{})
    end

    test "monitors multiple components correctly" do
      log1_pid = test_process()
      log2_pid = test_process()

      recovery_attempt = %{
        transaction_system_layout:
          create_layout(
            proxies: [test_process(), test_process()],
            resolvers: [{"key1", test_process()}, {"key2", test_process()}],
            logs: %{{:log, 1} => %{}, {:log, 2} => %{}},
            services: %{
              {:log, 1} => %{kind: :log, status: {:up, log1_pid}},
              {:log, 2} => %{kind: :log, status: {:up, log2_pid}},
              # Storage services shouldn't be monitored
              {:storage, 1} => %{kind: :storage, status: {:up, test_process()}}
            }
          )
      }

      execute_and_verify(recovery_attempt, %{})
    end

    test "excludes storage services from monitoring" do
      storage_pid = test_process()

      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt = %{
        transaction_system_layout:
          create_layout(
            services: %{
              {:log, 1} => %{kind: :log, status: {:up, test_process()}},
              {:storage, 1} => %{kind: :storage, status: {:up, storage_pid}}
            }
          )
      }

      execute_and_verify(recovery_attempt, %{monitor_fn: monitor_fn})

      # Should not receive monitoring message for storage PID
      refute_received {:monitored, ^storage_pid}
    end

    test "crashes if services are not in :up status" do
      down_log_pid = test_process()

      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt = %{
        transaction_system_layout:
          create_layout(
            logs: %{{:log, 1} => %{}, {:log, 2} => %{}},
            services: %{
              {:log, 1} => %{kind: :log, status: {:up, test_process()}},
              {:log, 2} => %{kind: :log, status: {:down, down_log_pid}}
            }
          )
      }

      # Should crash when trying to extract PID from :down service
      assert_raise FunctionClauseError, fn ->
        MonitoringPhase.execute(recovery_attempt, %{monitor_fn: monitor_fn})
      end
    end
  end
end
