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
              {:materializer, 1} => %{kind: :materializer, status: {:up, test_process()}}
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
              {:materializer, 1} => %{kind: :materializer, status: {:up, storage_pid}}
            }
          )
      }

      execute_and_verify(recovery_attempt, %{monitor_fn: monitor_fn})

      # Should not receive monitoring message for storage PID
      refute_received {:monitored, ^storage_pid}
    end

    test "monitors the metadata materializer (tag 0 is transaction-core)" do
      metadata_pid = test_process()

      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt = %{
        transaction_system_layout: Map.put(create_layout(), :metadata_materializer, metadata_pid)
      }

      execute_and_verify(recovery_attempt, %{monitor_fn: monitor_fn})

      assert_received {:monitored, ^metadata_pid}
    end

    test "does not monitor data-shard materializers (distributor-owned healing)" do
      metadata_pid = test_process()
      data_shard_pid = test_process()

      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt = %{
        transaction_system_layout:
          create_layout()
          |> Map.put(:metadata_materializer, metadata_pid)
          |> Map.put(:shard_materializers, %{0 => metadata_pid, 1 => data_shard_pid})
      }

      execute_and_verify(recovery_attempt, %{monitor_fn: monitor_fn})

      # The metadata materializer is monitored exactly once, even though the
      # same pid also appears as shard_materializers[0] (fresh-cluster case).
      assert_received {:monitored, ^metadata_pid}
      refute_received {:monitored, ^metadata_pid}
      # Data-shard materializer death is the distributor's job to heal.
      refute_received {:monitored, ^data_shard_pid}
    end

    test "director's mailbox sees :DOWN only for the metadata materializer" do
      metadata_pid = spawn(fn -> Process.sleep(:infinity) end)
      data_shard_pid = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt = %{
        transaction_system_layout:
          create_layout()
          |> Map.put(:metadata_materializer, metadata_pid)
          |> Map.put(:shard_materializers, %{0 => metadata_pid, 1 => data_shard_pid})
      }

      # Use the real Process.monitor so :DOWN routing lands in this process,
      # standing in for the director.
      execute_and_verify(recovery_attempt, %{})

      Process.exit(data_shard_pid, :kill)
      refute_receive {:DOWN, _ref, :process, ^data_shard_pid, _reason}, 100

      Process.exit(metadata_pid, :kill)
      assert_receive {:DOWN, _ref, :process, ^metadata_pid, :killed}
    end

    test "tolerates a layout without a metadata materializer" do
      monitor_fn = fn pid ->
        send(self(), {:monitored, pid})
        make_ref()
      end

      recovery_attempt = %{transaction_system_layout: create_layout()}

      execute_and_verify(recovery_attempt, %{monitor_fn: monitor_fn})
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
