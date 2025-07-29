defmodule Bedrock.ControlPlane.Director.ServiceLockingIntegrationTest do
  @moduledoc """
  Integration test for selective service locking behavior.

  This test verifies the fix for the service discovery race condition where
  Lock Services Phase was locking ALL available services instead of being selective.

  The test simulates the c1, c2, kill c2, start c2 scenario that exposed the bug.
  """

  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery
  alias Bedrock.ControlPlane.Director.Recovery.LockOldSystemServicesPhase
  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  import RecoveryTestSupport

  describe "Selective Service Locking" do
    test "epoch 1: no services locked initially, services locked during recruitment" do
      # Simulate first-time initialization (epoch 1)
      recovery_attempt = first_time_recovery()

      # Available services (simulating c1 startup)
      available_services = %{
        "bwecaxvz" => %{kind: :log, last_seen: {:log_worker_1, :node1}},
        "gb6cddk5" => %{kind: :storage, last_seen: {:storage_worker_1, :node1}},
        "kilvu2af" => %{kind: :log, last_seen: {:log_worker_2, :node1}},
        "zwtq7mfs" => %{kind: :storage, last_seen: {:storage_worker_2, :node1}}
      }

      # Empty old layout (first-time initialization)
      old_transaction_system_layout = %{
        logs: %{},
        storage_teams: []
      }

      context =
        create_test_context()
        |> Map.put(:available_services, available_services)
        |> Map.put(:old_transaction_system_layout, old_transaction_system_layout)
        |> with_multiple_nodes()
        |> with_mocked_service_locking()
        |> with_mocked_worker_creation()
        |> with_mocked_supervision()
        |> with_mocked_transactions()
        |> with_mocked_log_recovery()
        |> with_mocked_worker_management()

      # Execute lock services phase
      lock_phase_result =
        LockOldSystemServicesPhase.execute(
          %{recovery_attempt | state: :lock_old_system_services},
          context
        )

      # Should lock NO services initially (empty old layout)
      assert lock_phase_result.locked_service_ids == MapSet.new([])
      assert lock_phase_result.transaction_services == %{}
      assert lock_phase_result.state == :first_time_initialization
    end

    test "epoch 2: only old system services locked initially" do
      # Simulate recovery from existing cluster (epoch 2)
      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> with_log_recovery_info(%{
          "bwecaxvz" => %{kind: :log, oldest_version: 0, last_version: 5}
        })
        |> with_storage_recovery_info(%{
          "gb6cddk5" => %{kind: :storage, durable_version: 5, oldest_durable_version: 0}
        })

      # Available services (simulating c2 restart after c1 established system)
      available_services = %{
        "bwecaxvz" => %{kind: :log, last_seen: {:log_worker_1, :node1}},
        "gb6cddk5" => %{kind: :storage, last_seen: {:storage_worker_1, :node1}},
        # new node service
        "kilvu2af" => %{kind: :log, last_seen: {:log_worker_2, :node2}},
        # new node service
        "ukawgc4e" => %{kind: :storage, last_seen: {:storage_worker_3, :node2}},
        "zwtq7mfs" => %{kind: :storage, last_seen: {:storage_worker_2, :node1}}
      }

      # Old layout from epoch 1 (only bwecaxvz and gb6cddk5 were used)
      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 1, 2, 3, 4, 5]},
        storage_teams: [%{tag: 0, storage_ids: ["gb6cddk5"]}]
      }

      context =
        create_test_context()
        |> Map.put(:available_services, available_services)
        |> Map.put(:old_transaction_system_layout, old_transaction_system_layout)
        |> with_multiple_nodes()
        |> with_mocked_service_locking()

      # Execute lock services phase
      lock_phase_result =
        LockOldSystemServicesPhase.execute(
          %{recovery_attempt | state: :lock_old_system_services},
          context
        )

      # Should lock ONLY old system services (bwecaxvz, gb6cddk5)
      assert lock_phase_result.locked_service_ids == MapSet.new(["bwecaxvz", "gb6cddk5"])

      assert Map.keys(lock_phase_result.transaction_services) |> Enum.sort() == [
               "bwecaxvz",
               "gb6cddk5"
             ]

      assert lock_phase_result.state == :determine_old_logs_to_copy
    end

    test "recruitment phases should lock newly assigned services" do
      # This test verifies that recruitment phases lock services they assign
      # This is the missing piece that caused the validation failure

      # Start with recovery attempt that has completed lock services phase
      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> Map.put(:state, :recruit_logs_to_fill_vacancies)
        # Need to fill one log vacancy
        |> Map.put(:logs, %{{:vacancy, 1} => [0, 1]})
        # Old service already locked
        |> Map.put(:locked_service_ids, MapSet.new(["bwecaxvz"]))
        |> Map.put(:transaction_services, %{
          "bwecaxvz" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
        })

      available_services = %{
        # old service (locked)
        "bwecaxvz" => %{kind: :log, last_seen: {:log_1, :node1}},
        # new service (should be locked during recruitment)
        "kilvu2af" => %{kind: :log, last_seen: {:log_2, :node2}},
        "gb6cddk5" => %{kind: :storage, last_seen: {:storage_1, :node1}}
      }

      context =
        create_test_context()
        |> Map.put(:available_services, available_services)
        |> with_multiple_nodes()
        |> with_mocked_service_locking_that_tracks_calls()

      # Execute log recruitment phase
      alias Bedrock.ControlPlane.Director.Recovery.RecruitLogsToFillVacanciesPhase
      result = RecruitLogsToFillVacanciesPhase.execute(recovery_attempt, context)

      # Debug: Print the result
      IO.inspect(result, label: "Recruitment result")

      IO.inspect(Map.keys(result.transaction_services),
        label: "Transaction services after recruitment"
      )

      IO.inspect(result.logs, label: "Final logs assignment")

      # Should have recruited kilvu2af and added it to transaction_services
      assert result.state == :recruit_storage_to_fill_vacancies
      assert "kilvu2af" in Map.keys(result.transaction_services)

      # Most importantly: kilvu2af should have been locked during recruitment
      # (This is verified by the mocked locking function tracking calls)
      locked_services = get_locked_services_from_context(context)
      assert "kilvu2af" in locked_services
    end

    test "full recovery flow: selective locking + recruitment locking" do
      # This is the comprehensive test that should pass after fixing recruitment

      # Simulate the exact c1, c2, kill c2, start c2 scenario
      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> with_log_recovery_info(%{
          "bwecaxvz" => %{kind: :log, oldest_version: 0, last_version: 1}
        })
        |> with_storage_recovery_info(%{
          "gb6cddk5" => %{kind: :storage, durable_version: 0, oldest_durable_version: 0}
        })

      # c2 restart scenario: has both old and new services
      available_services = %{
        "bwecaxvz" => %{kind: :log, last_seen: {:log_1, :node1}},
        "gb6cddk5" => %{kind: :storage, last_seen: {:storage_1, :node1}},
        "kilvu2af" => %{kind: :log, last_seen: {:log_2, :node2}},
        "ukawgc4e" => %{kind: :storage, last_seen: {:storage_2, :node2}}
      }

      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 1]},
        storage_teams: [%{tag: 0, storage_ids: ["gb6cddk5"]}]
      }

      context =
        create_test_context()
        |> Map.put(:available_services, available_services)
        |> Map.put(:old_transaction_system_layout, old_transaction_system_layout)
        |> with_multiple_nodes()
        |> with_mocked_service_locking()
        |> with_mocked_worker_creation()
        |> with_mocked_supervision()
        |> with_mocked_transactions()
        |> with_mocked_log_recovery()
        |> with_mocked_worker_management()

      # Run full recovery
      result = Recovery.run_recovery_attempt(recovery_attempt, context)

      case result do
        {:ok, completed_attempt} ->
          # Success! Selective locking + recruitment locking worked
          assert completed_attempt.state == :completed

          # Should have both old and new services in final layout
          final_services = completed_attempt.transaction_system_layout.services
          # old service
          assert Map.has_key?(final_services, "bwecaxvz")
          # Should have recruited service (could be kilvu2af or created new one)
          assert map_size(final_services) >= 2

        {{:stalled, reason}, _stalled_attempt} ->
          flunk("Recovery should succeed with selective locking, but stalled: #{inspect(reason)}")
      end
    end
  end

  # Helper functions for mocking service locking that tracks calls
  defp with_mocked_service_locking_that_tracks_calls(context) do
    # Start a process to track locking calls
    {:ok, tracker} = Agent.start_link(fn -> [] end)

    lock_service_fn = fn service, _epoch ->
      Agent.update(tracker, fn locked -> [service.kind | locked] end)
      pid = spawn(fn -> :ok end)
      {:ok, pid, %{kind: service.kind, durable_version: 0, oldest_version: 0, last_version: 1}}
    end

    context
    |> Map.put(:lock_service_fn, lock_service_fn)
    |> Map.put(:lock_tracker, tracker)
  end

  defp get_locked_services_from_context(context) do
    case Map.get(context, :lock_tracker) do
      nil -> []
      tracker -> Agent.get(tracker, & &1)
    end
  end

  # Reuse existing test helpers
  defp with_mocked_service_locking(context) do
    lock_service_fn = fn service, _epoch ->
      pid = spawn(fn -> :ok end)
      {:ok, pid, %{kind: service.kind, durable_version: 0, oldest_version: 0, last_version: 1}}
    end

    Map.put(context, :lock_service_fn, lock_service_fn)
  end

  defp with_multiple_nodes(context) do
    Map.put(context, :node_list_fn, fn -> [:node1, :node2, :node3] end)
  end

  defp with_mocked_worker_creation(context) do
    create_worker_fn = fn _foreman_ref, worker_id, _kind, _opts ->
      {:ok, "#{worker_id}_ref"}
    end

    worker_info_fn = fn {worker_ref, _node}, _fields, _opts ->
      worker_id = String.replace(worker_ref, "_ref", "")

      {:ok,
       [
         id: worker_id,
         otp_name: String.to_atom(worker_id),
         kind: :log,
         pid: spawn(fn -> :ok end)
       ]}
    end

    context
    |> Map.put(:create_worker_fn, create_worker_fn)
    |> Map.put(:worker_info_fn, worker_info_fn)
  end

  defp with_mocked_supervision(context) do
    start_supervised_fn = fn _child_spec, _node ->
      {:ok,
       spawn(fn ->
         receive do
           {:"$gen_call", from, {:recover_from, _token, _logs, _first, _last}} ->
             GenServer.reply(from, {:ok, self()})

           _ ->
             :ok
         after
           5000 -> :ok
         end
       end)}
    end

    Map.put(context, :start_supervised_fn, start_supervised_fn)
  end

  defp with_mocked_transactions(context) do
    commit_transaction_fn = fn _proxy, _transaction -> {:ok, 101} end
    unlock_commit_proxy_fn = fn _proxy, _lock_token, _layout -> :ok end
    unlock_storage_fn = fn _storage_pid, _durable_version, _layout -> :ok end

    context
    |> Map.put(:commit_transaction_fn, commit_transaction_fn)
    |> Map.put(:unlock_commit_proxy_fn, unlock_commit_proxy_fn)
    |> Map.put(:unlock_storage_fn, unlock_storage_fn)
  end

  defp with_mocked_log_recovery(context) do
    log_recover_fn = fn _new_log_id,
                        _old_log_id,
                        _first_version,
                        _last_version,
                        _recovery_attempt ->
      {:ok, spawn(fn -> :ok end)}
    end

    Map.put(context, :log_recover_fn, log_recover_fn)
  end

  defp with_mocked_worker_management(context) do
    foreman_all_fn = fn _foreman_ref, _opts -> {:ok, []} end

    remove_workers_fn = fn _foreman_ref, worker_ids, _opts ->
      worker_ids |> Enum.map(&{&1, :ok}) |> Map.new()
    end

    monitor_fn = fn pid -> Process.monitor(pid) end

    context
    |> Map.put(:foreman_all_fn, foreman_all_fn)
    |> Map.put(:remove_workers_fn, remove_workers_fn)
    |> Map.put(:monitor_fn, monitor_fn)
  end
end
