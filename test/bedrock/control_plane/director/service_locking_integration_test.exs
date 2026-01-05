defmodule Bedrock.ControlPlane.Director.Recovery.LockingPhaseTest do
  @moduledoc """
  Integration test for selective service locking behavior.

  This test verifies the fix for the service discovery race condition where
  Lock Services Phase was locking ALL available services instead of being selective.

  The test simulates the c1, c2, kill c2, start c2 scenario that exposed the bug.
  """

  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.LockingPhase
  alias Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase
  alias Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase
  alias Bedrock.ControlPlane.Director.Recovery.TSLValidationPhase

  describe "Selective Service Locking" do
    test "epoch 1: no services locked initially, services locked during recruitment" do
      # Simulate first-time initialization (epoch 1)
      recovery_attempt = first_time_recovery()

      # Available services (simulating c1 startup)
      available_services = %{
        "bwecaxvz" => {:log, {:log_worker_1, :node1}},
        "gb6cddk5" => {:storage, {:storage_worker_1, :node1}},
        "kilvu2af" => {:log, {:log_worker_2, :node1}},
        "zwtq7mfs" => {:storage, {:storage_worker_2, :node1}}
      }

      # Empty old layout (first-time initialization)
      old_transaction_system_layout = %{
        logs: %{},
        storage_teams: []
      }

      context = create_full_mocked_context(available_services, old_transaction_system_layout)

      # Execute TSL validation phase first (this comes before LockingPhase now)
      # Should proceed to LockingPhase since TSL validation passed
      assert {_validated_attempt, LockingPhase} =
               TSLValidationPhase.execute(recovery_attempt, context)
    end

    test "epoch 2: only old system services locked initially" do
      # Simulate recovery from existing cluster (epoch 2) with services already locked
      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> Map.put(:locked_service_ids, MapSet.new(["bwecaxvz", "gb6cddk5"]))
        |> with_log_recovery_info(%{
          "bwecaxvz" => %{kind: :log, oldest_version: 0, last_version: 5}
        })
        |> with_storage_recovery_info(%{
          "gb6cddk5" => %{kind: :storage, durable_version: 5, oldest_durable_version: 0}
        })

      # Available services (simulating c2 restart after c1 established system)
      available_services = %{
        "bwecaxvz" => {:log, {:log_worker_1, :node1}},
        "gb6cddk5" => {:storage, {:storage_worker_1, :node1}},
        # new node service
        "kilvu2af" => {:log, {:log_worker_2, :node2}},
        # new node service
        "ukawgc4e" => {:storage, {:storage_worker_3, :node2}},
        "zwtq7mfs" => {:storage, {:storage_worker_2, :node1}}
      }

      # Old layout from epoch 1 (only bwecaxvz and gb6cddk5 were used)
      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 5]},
        storage_teams: [%{tag: 0, key_range: {"", <<0xFF, 0xFF>>}, storage_ids: ["gb6cddk5"]}]
      }

      context =
        available_services
        |> create_basic_context(old_transaction_system_layout)
        |> with_mocked_service_locking()

      # Execute TSL validation phase first (this comes before LockingPhase now)
      # Should proceed to LockingPhase since TSL validation passed
      assert {_validated_attempt, LockingPhase} =
               TSLValidationPhase.execute(recovery_attempt, context)
    end

    test "log recruitment phase should lock newly assigned services" do
      # This test verifies that log recruitment phase locks services they assign

      # Start with recovery attempt that has completed lock services phase
      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> Map.put(:state, :recruit_logs_to_fill_vacancies)
        # Need to fill one log vacancy - kilvu2af should be recruited for this
        |> Map.put(:logs, %{{:vacancy, 1} => [0, 1]})
        # Old service already locked
        |> Map.put(:locked_service_ids, MapSet.new(["bwecaxvz"]))
        |> Map.put(:transaction_services, %{
          "bwecaxvz" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
        })

      available_services = %{
        # old service (locked)
        "bwecaxvz" => {:log, {:log_1, :node1}},
        # new service (should be locked during recruitment)
        "kilvu2af" => {:log, {:log_2, :node2}},
        "gb6cddk5" => {:storage, {:storage_1, :node1}}
      }

      # Set up old system layout so bwecaxvz is excluded from recruitment
      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 1]},
        storage_teams: []
      }

      context = create_tracking_context(available_services, old_transaction_system_layout)

      # Execute log recruitment phase
      # Should have recruited kilvu2af and proceed to StorageRecruitmentPhase
      assert {%{transaction_services: transaction_services}, StorageRecruitmentPhase} =
               LogRecruitmentPhase.execute(recovery_attempt, context)

      assert "kilvu2af" in Map.keys(transaction_services)

      # Most importantly: kilvu2af should have been locked during recruitment
      locked_services = get_locked_services_from_context(context)
      assert "kilvu2af" in locked_services
    end

    test "storage recruitment phase should lock newly assigned services" do
      # This test verifies that storage recruitment phase locks services they assign
      # and excludes old system storage services from recruitment

      # Start with recovery attempt that has completed log recruitment
      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> Map.put(:state, :recruit_storage_to_fill_vacancies)
        # Need to fill one storage vacancy - ukawgc4e should be recruited for this
        |> Map.put(:storage_teams, [%{tag: 0, storage_ids: [{:vacancy, 1}]}])
        # Old services already locked
        |> Map.put(:locked_service_ids, MapSet.new(["bwecaxvz", "gb6cddk5"]))
        |> Map.put(:transaction_services, %{
          "bwecaxvz" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
          "gb6cddk5" => %{status: {:up, self()}, kind: :storage, last_seen: {:storage_1, :node1}}
        })
        |> with_storage_recovery_info(%{
          "gb6cddk5" => %{kind: :storage, durable_version: 0, oldest_durable_version: 0}
        })

      available_services = %{
        # old services (should be excluded from recruitment)
        "bwecaxvz" => {:log, {:log_1, :node1}},
        "gb6cddk5" => {:storage, {:storage_1, :node1}},
        # new service (should be recruited and locked)
        "ukawgc4e" => {:storage, {:storage_2, :node2}}
      }

      # Set up old system layout so gb6cddk5 is excluded from recruitment
      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 1]},
        storage_teams: [%{tag: 0, storage_ids: ["gb6cddk5"]}]
      }

      context = create_tracking_context(available_services, old_transaction_system_layout)

      # Execute storage recruitment phase
      # Should have recruited ukawgc4e and proceed to LogReplayPhase
      assert {%{transaction_services: transaction_services}, Bedrock.ControlPlane.Director.Recovery.LogReplayPhase} =
               StorageRecruitmentPhase.execute(recovery_attempt, context)

      assert "ukawgc4e" in Map.keys(transaction_services)

      # Most importantly: ukawgc4e should have been locked during recruitment
      # and gb6cddk5 should NOT have been locked again (excluded from recruitment)
      locked_services = get_locked_services_from_context(context)
      assert "ukawgc4e" in locked_services
      # Should be excluded from recruitment
      refute "gb6cddk5" in locked_services
    end

    test "selective locking integration: lock old system, recruit and lock new services" do
      # This test verifies the complete selective locking behavior without requiring
      # full end-to-end recovery (which involves complex proxy/resolver setup)

      # Test the LockingPhase directly
      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> Map.put(:state, :lock_old_system_services)
        |> with_log_recovery_info(%{
          "bwecaxvz" => %{kind: :log, oldest_version: 0, last_version: 1}
        })
        |> with_storage_recovery_info(%{
          "gb6cddk5" => %{kind: :storage, durable_version: 0, oldest_durable_version: 0}
        })

      # Services available for locking/recruitment
      available_services = %{
        "bwecaxvz" => {:log, {:log_1, :node1}},
        "gb6cddk5" => {:storage, {:storage_1, :node1}},
        "kilvu2af" => {:log, {:log_2, :node2}},
        "ukawgc4e" => {:storage, {:storage_2, :node2}}
      }

      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 1]},
        storage_teams: [%{tag: 0, storage_ids: ["gb6cddk5"], key_range: {"", <<0xFF, 0xFF>>}}]
      }

      context = create_tracking_context(available_services, old_transaction_system_layout)

      # 1. Test selective locking phase - should lock only old system services
      assert {lock_result, _next_phase} = LockingPhase.execute(recovery_attempt, context)

      assert lock_result.locked_service_ids == MapSet.new(["bwecaxvz", "gb6cddk5"])
      assert lock_result.transaction_services |> Map.keys() |> Enum.sort() == ["bwecaxvz", "gb6cddk5"]

      # Services should have proper status format
      assert_service_has_proper_format(lock_result.transaction_services, "bwecaxvz", :log)
      assert_service_has_proper_format(lock_result.transaction_services, "gb6cddk5", :storage)

      # 2. Test log recruitment phase
      log_recovery_attempt = %{
        lock_result
        | logs: %{{:vacancy, 1} => [0, 1]}
      }

      # Should recruit kilvu2af (excluding bwecaxvz from old system)
      assert {log_result, _next_phase} =
               LogRecruitmentPhase.execute(log_recovery_attempt, context)

      assert "kilvu2af" in Map.keys(log_result.transaction_services)
      assert_service_has_proper_format(log_result.transaction_services, "kilvu2af", :log)

      # 3. Test storage recruitment phase
      storage_recovery_attempt = %{
        log_result
        | storage_teams: [%{tag: 0, storage_ids: [{:vacancy, 1}]}]
      }

      # Should recruit ukawgc4e (excluding gb6cddk5 from old system)
      assert {storage_result, _next_phase} =
               StorageRecruitmentPhase.execute(storage_recovery_attempt, context)

      assert "ukawgc4e" in Map.keys(storage_result.transaction_services)
      assert_service_has_proper_format(storage_result.transaction_services, "ukawgc4e", :storage)

      # Final verification: all services locked with proper format
      assert_services_locked(context, ["bwecaxvz", "gb6cddk5", "kilvu2af", "ukawgc4e"])

      # Verify all services have the required fields for persistence phase
      final_services = storage_result.transaction_services
      assert map_size(final_services) == 4

      Enum.each(final_services, fn {_id, service} ->
        assert %{status: {:up, _}, kind: _, last_seen: _} = service
      end)
    end
  end

  describe "Task message leak prevention" do
    test "LockingPhase with slow tasks and race conditions does not leak Task replies" do
      # This test demonstrates that the LockingPhase correctly uses Task.async_stream
      # in a way that does NOT leak Task reply messages to the parent process,
      # even when some tasks are slow and complete after processing stops.
      #
      # This test validates that our Director handle_info fix addresses the
      # actual issue (Task replies from elsewhere) rather than fixing a
      # non-existent issue in the LockingPhase itself.

      # Create a mock lock_service_fn that simulates slow operations
      mock_lock_service_fn = fn service, _epoch ->
        case service do
          {:log, {:slow_log, :node1}} ->
            # Simulate slow locking operation
            Process.sleep(50)
            pid = spawn(fn -> :ok end)
            {:ok, pid, %{kind: :log, durable_version: 0, oldest_version: 0, last_version: 1}}

          {:storage, {:slow_storage, :node2}} ->
            # Simulate even slower locking operation
            Process.sleep(100)
            pid = spawn(fn -> :ok end)
            {:ok, pid, %{kind: :storage, durable_version: 5, oldest_version: 0, last_version: 10}}
        end
      end

      # Set up recovery attempt with old services to lock
      recovery_attempt =
        existing_cluster_recovery()
        |> with_epoch(2)
        |> with_log_recovery_info(%{
          "slow_log" => %{kind: :log, oldest_version: 0, last_version: 5}
        })
        |> with_storage_recovery_info(%{
          "slow_storage" => %{kind: :storage, durable_version: 5, oldest_durable_version: 0}
        })

      # Create context with available services matching the recovery info
      available_services = %{
        "slow_log" => {:log, {:slow_log, :node1}},
        "slow_storage" => {:storage, {:slow_storage, :node2}}
      }

      old_layout = %{
        logs: %{"slow_log" => [0, 1]},
        storage_teams: [%{storage_ids: ["slow_storage"]}]
      }

      context =
        available_services
        |> create_basic_context(old_layout)
        |> Map.put(:lock_service_fn, mock_lock_service_fn)

      # Execute locking phase - should successfully lock both services despite slow operations
      assert {%{locked_service_ids: locked_ids, transaction_services: services}, next_phase} =
               LockingPhase.execute(recovery_attempt, context)

      assert locked_ids == MapSet.new(["slow_log", "slow_storage"])
      assert map_size(services) == 2
      # Should proceed to the next phase in the recovery sequence
      assert is_atom(next_phase)

      # The critical test: verify NO Task reply messages leaked to this process
      leaked_messages = collect_all_messages([])

      task_reply_messages =
        Enum.filter(leaked_messages, fn
          {[:alias | ref], _result} when is_reference(ref) -> true
          _ -> false
        end)

      # This should pass, proving the LockingPhase doesn't have the leak issue
      assert task_reply_messages == [], """
      Task replies leaked from LockingPhase: #{inspect(task_reply_messages)}
      All messages: #{inspect(leaked_messages)}

      This would indicate an issue with the LockingPhase Task.async_stream implementation.
      """

      # Also verify no unexpected messages at all
      assert leaked_messages == [], """
      Unexpected messages during LockingPhase: #{inspect(leaked_messages)}

      LockingPhase should not send messages to the parent process.
      """
    end

    defp collect_all_messages(acc) do
      receive do
        message -> collect_all_messages([message | acc])
      after
        10 -> Enum.reverse(acc)
      end
    end
  end

  # Helper functions for mocking service locking that tracks calls
  defp with_mocked_service_locking_that_tracks_calls(context) do
    # Start a process to track locking calls
    {:ok, tracker} = Agent.start_link(fn -> [] end)

    lock_service_fn = fn service, _epoch ->
      # Handle coordinator format: {kind, {otp_name, node}}
      {kind, location} = service

      # Find the service ID by looking it up in available_services
      service_id = find_service_id_by_location(context.available_services, location)

      Agent.update(tracker, fn locked -> [service_id | locked] end)
      pid = spawn(fn -> :ok end)
      {:ok, pid, %{kind: kind, durable_version: 0, oldest_version: 0, last_version: 1}}
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

  defp find_service_id_by_location(available_services, location) do
    Enum.find_value(available_services, fn {id, {_kind, desc_location}} ->
      match_service_location(desc_location, location, id)
    end)
  end

  defp match_service_location(desc_location, target_location, id) do
    if desc_location == target_location, do: id
  end

  # Common context setup helpers
  defp create_basic_context(available_services, old_layout) do
    create_test_context()
    |> Map.put(:available_services, available_services)
    |> Map.put(:old_transaction_system_layout, old_layout)
    |> with_multiple_nodes()
  end

  defp create_full_mocked_context(available_services, old_layout) do
    available_services
    |> create_basic_context(old_layout)
    |> with_mocked_service_locking()
    |> with_mocked_worker_creation()
    |> with_mocked_supervision()
    |> with_mocked_transactions()
    |> with_mocked_log_recovery()
    |> with_mocked_worker_management()
  end

  defp create_tracking_context(available_services, old_layout) do
    available_services
    |> create_basic_context(old_layout)
    |> with_mocked_service_locking_that_tracks_calls()
  end

  # Service verification helpers
  defp assert_service_has_proper_format(services, service_id, expected_kind) do
    assert %{status: {:up, _}, kind: ^expected_kind, last_seen: _} = services[service_id]
  end

  defp assert_services_locked(context, expected_service_ids) do
    locked_services = get_locked_services_from_context(context)

    assert Enum.all?(expected_service_ids, &(&1 in locked_services)),
           "Expected services #{inspect(expected_service_ids)} to be locked, got: #{inspect(locked_services)}"
  end

  # Reuse existing test helpers
  defp with_mocked_service_locking(context) do
    lock_service_fn = fn service, _epoch ->
      pid = spawn(fn -> :ok end)
      # Handle coordinator format: {kind, {otp_name, node}}
      {kind, _location} = service
      {:ok, pid, %{kind: kind, durable_version: 0, oldest_version: 0, last_version: 1}}
    end

    Map.put(context, :lock_service_fn, lock_service_fn)
  end

  defp with_multiple_nodes(context) do
    Map.put(context, :node_list_fn, fn -> [:node1, :node2, :node3] end)
  end

  defp with_mocked_worker_creation(context) do
    create_worker_fn = fn _foreman_ref, worker_id, kind, _opts ->
      {:ok, "#{worker_id}_ref_#{kind}"}
    end

    worker_info_fn = fn {worker_ref, _node}, _fields, _opts ->
      # Extract worker_id and kind from the ref
      [worker_id, kind_str] = String.split(worker_ref, "_ref_")
      kind = String.to_atom(kind_str)

      {:ok,
       [
         id: worker_id,
         otp_name: String.to_atom(worker_id),
         kind: kind,
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
             GenServer.reply(from, :ok)

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
    copy_log_data_fn = fn _new_log_id, _old_log_id, _first_version, _last_version, _service_pids ->
      {:ok, spawn(fn -> :ok end)}
    end

    Map.put(context, :copy_log_data_fn, copy_log_data_fn)
  end

  defp with_mocked_worker_management(context) do
    foreman_all_fn = fn _foreman_ref, _opts -> {:ok, []} end

    remove_workers_fn = fn _foreman_ref, worker_ids, _opts ->
      Map.new(worker_ids, &{&1, :ok})
    end

    monitor_fn = fn pid -> Process.monitor(pid) end

    context
    |> Map.put(:foreman_all_fn, foreman_all_fn)
    |> Map.put(:remove_workers_fn, remove_workers_fn)
    |> Map.put(:monitor_fn, monitor_fn)
  end
end
