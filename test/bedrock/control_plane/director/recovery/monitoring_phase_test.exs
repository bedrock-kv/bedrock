defmodule Bedrock.ControlPlane.Director.Recovery.MonitoringPhaseTest do
  use ExUnit.Case, async: true
  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.MonitoringPhase

  # Helper to create test transaction system layout
  defp create_layout(opts \\ []) do
    sequencer = Keyword.get(opts, :sequencer, spawn(fn -> :timer.sleep(100) end))
    proxies = Keyword.get(opts, :proxies, [spawn(fn -> :timer.sleep(100) end)])
    resolvers = Keyword.get(opts, :resolvers, [{"start_key", spawn(fn -> :timer.sleep(100) end)}])
    logs = Keyword.get(opts, :logs, %{{:log, 1} => %{}})

    # Create services map based on the components
    services =
      Keyword.get(opts, :services, %{
        {:log, 1} => %{kind: :log, status: {:up, spawn(fn -> :timer.sleep(100) end)}}
      })

    %{
      sequencer: sequencer,
      proxies: proxies,
      resolvers: resolvers,
      logs: logs,
      services: services
    }
  end

  describe "execute/1" do
    test "monitors all components when all PIDs are valid" do
      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt =
        recovery_attempt()
        |> with_state(:monitor_components)
        |> Map.put(:transaction_system_layout, create_layout())

      result = MonitoringPhase.execute(recovery_attempt, %{monitor_fn: monitor_fn})
      assert result.state == :completed

      # Should have monitored sequencer, 1 proxy, 1 resolver, 1 log
      assert_received {:monitored, _}
      assert_received {:monitored, _}
      assert_received {:monitored, _}
      assert_received {:monitored, _}
    end

    test "uses default Process.monitor when no monitor_fn provided" do
      recovery_attempt =
        recovery_attempt()
        |> with_state(:monitor_components)
        |> Map.put(:transaction_system_layout, create_layout())

      result = MonitoringPhase.execute(recovery_attempt, %{})
      assert result.state == :completed
    end

    test "monitors multiple components correctly" do
      log1_pid = spawn(fn -> :timer.sleep(100) end)
      log2_pid = spawn(fn -> :timer.sleep(100) end)

      recovery_attempt = %{
        state: :monitor_components,
        transaction_system_layout:
          create_layout(
            proxies: [spawn(fn -> :timer.sleep(100) end), spawn(fn -> :timer.sleep(100) end)],
            resolvers: [
              {"key1", spawn(fn -> :timer.sleep(100) end)},
              {"key2", spawn(fn -> :timer.sleep(100) end)}
            ],
            logs: %{
              {:log, 1} => %{},
              {:log, 2} => %{}
            },
            services: %{
              {:log, 1} => %{kind: :log, status: {:up, log1_pid}},
              {:log, 2} => %{kind: :log, status: {:up, log2_pid}},
              # Storage services shouldn't be monitored 
              {:storage, 1} => %{
                kind: :storage,
                status: {:up, spawn(fn -> :timer.sleep(100) end)}
              }
            }
          )
      }

      result = MonitoringPhase.execute(recovery_attempt, %{})
      assert result.state == :completed
    end

    test "excludes storage services from monitoring" do
      storage_pid = spawn(fn -> :timer.sleep(100) end)

      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt = %{
        state: :monitor_components,
        transaction_system_layout:
          create_layout(
            services: %{
              {:log, 1} => %{kind: :log, status: {:up, spawn(fn -> :timer.sleep(100) end)}},
              {:storage, 1} => %{kind: :storage, status: {:up, storage_pid}}
            }
          )
      }

      MonitoringPhase.execute(recovery_attempt, %{monitor_fn: monitor_fn})

      # Should not receive monitoring message for storage PID
      refute_received {:monitored, ^storage_pid}
    end

    test "crashes if services are not in :up status" do
      down_log_pid = spawn(fn -> :timer.sleep(100) end)

      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt = %{
        state: :monitor_components,
        transaction_system_layout:
          create_layout(
            logs: %{
              {:log, 1} => %{},
              {:log, 2} => %{}
            },
            services: %{
              {:log, 1} => %{kind: :log, status: {:up, spawn(fn -> :timer.sleep(100) end)}},
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
