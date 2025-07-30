defmodule Bedrock.ControlPlane.Director.ServiceLockingIntegrationTest do
  @moduledoc """
  Integration test for selective service locking behavior.

  This test verifies the fix for the service discovery race condition where
  Lock Services Phase was locking ALL available services instead of being selective.

  The test simulates the c1, c2, kill c2, start c2 scenario that exposed the bug.
  """

  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Recovery.LockingPhase
  alias Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase
  alias Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase

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
      {lock_phase_result, next_phase} =
        LockingPhase.execute(
          recovery_attempt,
          context
        )

      # Should lock NO services initially (empty old layout)
      assert lock_phase_result.locked_service_ids == MapSet.new([])
      assert lock_phase_result.transaction_services == %{}
      assert next_phase == Bedrock.ControlPlane.Director.Recovery.InitializationPhase
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
      {lock_phase_result, next_phase} =
        LockingPhase.execute(
          recovery_attempt,
          context
        )

      # Should lock ONLY old system services (bwecaxvz, gb6cddk5)
      assert lock_phase_result.locked_service_ids == MapSet.new(["bwecaxvz", "gb6cddk5"])

      assert Map.keys(lock_phase_result.transaction_services) |> Enum.sort() == [
               "bwecaxvz",
               "gb6cddk5"
             ]

      assert next_phase == Bedrock.ControlPlane.Director.Recovery.LogDiscoveryPhase
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
        "bwecaxvz" => %{kind: :log, last_seen: {:log_1, :node1}},
        # new service (should be locked during recruitment)
        "kilvu2af" => %{kind: :log, last_seen: {:log_2, :node2}},
        "gb6cddk5" => %{kind: :storage, last_seen: {:storage_1, :node1}}
      }

      # Set up old system layout so bwecaxvz is excluded from recruitment
      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 1]},
        storage_teams: []
      }

      context =
        create_test_context()
        |> Map.put(:available_services, available_services)
        |> Map.put(:old_transaction_system_layout, old_transaction_system_layout)
        |> with_multiple_nodes()
        |> with_mocked_service_locking_that_tracks_calls()

      # Execute log recruitment phase
      {result, next_phase} = LogRecruitmentPhase.execute(recovery_attempt, context)

      # Should have recruited kilvu2af and added it to transaction_services
      assert next_phase == Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase
      assert "kilvu2af" in Map.keys(result.transaction_services)

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
        "bwecaxvz" => %{kind: :log, last_seen: {:log_1, :node1}},
        "gb6cddk5" => %{kind: :storage, last_seen: {:storage_1, :node1}},
        # new service (should be recruited and locked)
        "ukawgc4e" => %{kind: :storage, last_seen: {:storage_2, :node2}}
      }

      # Set up old system layout so gb6cddk5 is excluded from recruitment
      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 1]},
        storage_teams: [%{tag: 0, storage_ids: ["gb6cddk5"]}]
      }

      context =
        create_test_context()
        |> Map.put(:available_services, available_services)
        |> Map.put(:old_transaction_system_layout, old_transaction_system_layout)
        |> with_multiple_nodes()
        |> with_mocked_service_locking_that_tracks_calls()

      # Execute storage recruitment phase
      {result, next_phase} = StorageRecruitmentPhase.execute(recovery_attempt, context)

      # Should have recruited ukawgc4e and added it to transaction_services
      assert next_phase == Bedrock.ControlPlane.Director.Recovery.LogReplayPhase
      assert "ukawgc4e" in Map.keys(result.transaction_services)

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
        "bwecaxvz" => %{kind: :log, last_seen: {:log_1, :node1}},
        "gb6cddk5" => %{kind: :storage, last_seen: {:storage_1, :node1}},
        "kilvu2af" => %{kind: :log, last_seen: {:log_2, :node2}},
        "ukawgc4e" => %{kind: :storage, last_seen: {:storage_2, :node2}}
      }

      old_transaction_system_layout = %{
        logs: %{"bwecaxvz" => [0, 1]},
        storage_teams: [%{tag: 0, storage_ids: ["gb6cddk5"], key_range: {"", :end}}]
      }

      context =
        create_test_context()
        |> Map.put(:available_services, available_services)
        |> Map.put(:old_transaction_system_layout, old_transaction_system_layout)
        |> with_multiple_nodes()
        |> with_mocked_service_locking_that_tracks_calls()

      # 1. Test selective locking phase
      {lock_result, _next_phase} = LockingPhase.execute(recovery_attempt, context)

      # Should lock only old system services
      assert lock_result.locked_service_ids == MapSet.new(["bwecaxvz", "gb6cddk5"])
      assert Map.keys(lock_result.transaction_services) |> Enum.sort() == ["bwecaxvz", "gb6cddk5"]

      # Services should have proper status format
      assert %{status: {:up, _}, kind: :log} = lock_result.transaction_services["bwecaxvz"]
      assert %{status: {:up, _}, kind: :storage} = lock_result.transaction_services["gb6cddk5"]

      # 2. Test log recruitment phase
      log_recovery_attempt = %{
        lock_result
        | logs: %{{:vacancy, 1} => [0, 1]}
      }

      {log_result, _next_phase} = LogRecruitmentPhase.execute(log_recovery_attempt, context)

      # Should recruit kilvu2af (excluding bwecaxvz from old system)
      assert "kilvu2af" in Map.keys(log_result.transaction_services)
      assert %{status: {:up, _}, kind: :log} = log_result.transaction_services["kilvu2af"]

      # 3. Test storage recruitment phase
      storage_recovery_attempt = %{
        log_result
        | storage_teams: [%{tag: 0, storage_ids: [{:vacancy, 1}]}]
      }

      {storage_result, _next_phase} =
        StorageRecruitmentPhase.execute(storage_recovery_attempt, context)

      # Should recruit ukawgc4e (excluding gb6cddk5 from old system)
      assert "ukawgc4e" in Map.keys(storage_result.transaction_services)
      assert %{status: {:up, _}, kind: :storage} = storage_result.transaction_services["ukawgc4e"]

      # Final verification: all services locked with proper format
      locked_services = get_locked_services_from_context(context)
      # old system log
      assert "bwecaxvz" in locked_services
      # old system storage
      assert "gb6cddk5" in locked_services
      # recruited log
      assert "kilvu2af" in locked_services
      # recruited storage
      assert "ukawgc4e" in locked_services

      # Verify all services have the required fields for persistence phase
      final_services = storage_result.transaction_services
      assert map_size(final_services) == 4

      Enum.each(final_services, fn {_id, service} ->
        assert Map.has_key?(service, :status)
        assert Map.has_key?(service, :kind)
        assert Map.has_key?(service, :last_seen)
        assert match?({:up, _}, service.status)
      end)
    end
  end

  # Helper functions for mocking service locking that tracks calls
  defp with_mocked_service_locking_that_tracks_calls(context) do
    # Start a process to track locking calls
    {:ok, tracker} = Agent.start_link(fn -> [] end)

    lock_service_fn = fn service, _epoch ->
      # Find the service ID by looking it up in available_services
      service_id =
        context.available_services
        |> Enum.find_value(fn {id, desc} ->
          if desc.last_seen == service.last_seen, do: id, else: nil
        end)

      Agent.update(tracker, fn locked -> [service_id | locked] end)
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
    create_worker_fn = fn _foreman_ref, worker_id, kind, _opts ->
      {:ok, "#{worker_id}_ref_#{kind}"}
    end

    worker_info_fn = fn {worker_ref, _node}, _fields, _opts ->
      # Extract worker_id and kind from the ref
      [worker_id, kind_str] = worker_ref |> String.split("_ref_")
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
