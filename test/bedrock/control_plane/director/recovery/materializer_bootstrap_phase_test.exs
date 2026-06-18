defmodule Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase
  alias Bedrock.DataPlane.Version

  describe "execute/2" do
    test "for fresh cluster, creates default shard layout and materializers" do
      system_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)

      # Track which shards we create materializers for
      created_shards = :ets.new(:created_shards, [:bag, :public])

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})

      # Fresh cluster context - no old logs, but with materializer capability
      context =
        [
          old_transaction_system_layout: %{logs: %{}},
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          {:ok, :new_materializer_ref}
        end)
        |> Map.put(:lock_materializer_fn, fn {:materializer, _ref, shard_tag}, _epoch ->
          :ets.insert(created_shards, {:shard, shard_tag})
          # Return different PIDs for different shards
          pid = if shard_tag == 0, do: system_materializer_pid, else: user_materializer_pid
          {:ok, pid}
        end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          # Should have default shard layout for fresh cluster
          assert updated_attempt.shard_layout
          assert is_map(updated_attempt.shard_layout)

          # Default layout has two shards: system and user
          assert map_size(updated_attempt.shard_layout) == 2

          # Should have created materializers for both shards
          assert map_size(updated_attempt.shard_materializers) == 2
          assert Map.has_key?(updated_attempt.shard_materializers, 0)
          assert Map.has_key?(updated_attempt.shard_materializers, 1)

          # metadata_materializer should be the system shard materializer
          assert updated_attempt.metadata_materializer == system_materializer_pid
        end)

      assert log =~ "Fresh cluster detected"

      # Verify both shards were created
      shards = created_shards |> :ets.lookup(:shard) |> Enum.map(fn {:shard, tag} -> tag end)
      assert 0 in shards
      assert 1 in shards

      :ets.delete(created_shards)
    end

    test "for fresh cluster, unlocks shard materializers with empty log descriptors" do
      system_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)

      logs = %{
        {:vacancy, 1} => [],
        {:vacancy, 2} => []
      }

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, logs)

      unlocks = :ets.new(:materializer_unlocks, [:bag, :public])

      context =
        [
          old_transaction_system_layout: %{logs: %{}},
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          {:ok, :new_materializer_ref}
        end)
        |> Map.put(:lock_materializer_fn, fn {:materializer, _ref, shard_tag}, _epoch ->
          pid = if shard_tag == 0, do: system_materializer_pid, else: user_materializer_pid
          {:ok, pid}
        end)
        |> Map.put(:unlock_materializer_fn, fn pid, _version, tsl ->
          :ets.insert(unlocks, {:unlock, pid, tsl.logs})
          :ok
        end)

      assert {updated_attempt, CommitProxyStartupPhase} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)

      assert updated_attempt.metadata_materializer == system_materializer_pid

      assert {:unlock, system_materializer_pid, logs} in :ets.lookup(unlocks, :unlock)
      assert {:unlock, user_materializer_pid, logs} in :ets.lookup(unlocks, :unlock)

      :ets.delete(unlocks)
    end

    test "stalls when no materializer capable nodes exist" do
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})

      # Existing cluster - has old logs but no materializer in available_services
      # AND no materializer capability in nodes
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            storage: [Node.self()]
            # Note: no :materializer capability
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})

      log =
        capture_log(fn ->
          assert {_attempt, {:stalled, :no_materializer_capable_nodes}} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)
        end)

      assert log =~ "System shard materializer not found, creating new one"
    end

    test "uses existing materializer from available_services (legacy format)" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      # Mock context with materializer available and all required functions
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:materializer, {:test_materializer, node()}}
        })
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          {:ok, :new_materializer_ref}
        end)
        |> Map.put(:lock_materializer_fn, fn
          {:materializer, {:test_materializer, _node}}, _epoch ->
            {:ok, materializer_pid}

          {:materializer, {:new_materializer_ref, _node}, 1}, _epoch ->
            {:ok, user_materializer_pid}
        end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:durable_version] ->
          {:ok, %{durable_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.metadata_materializer == materializer_pid
          assert updated_attempt.shard_layout
          assert updated_attempt.shard_materializers == %{0 => materializer_pid, 1 => user_materializer_pid}
        end)

      assert log =~ "Materializer caught up to version"
      assert log =~ "Shard 1 materializer not found, creating new one"
    end

    test "uses materializer from available_services (shard-based format)" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      # New format: {kind, ref, shard_id}
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "mat_sys_0" => {:materializer, {:test_materializer, node()}, 0},
          "mat_user_1" => {:materializer, {:user_materializer, node()}, 1}
        })
        |> Map.put(:lock_materializer_fn, fn
          {:materializer, {:test_materializer, _node}, 0}, _epoch -> {:ok, materializer_pid}
          {:materializer, {:user_materializer, _node}, 1}, _epoch -> {:ok, user_materializer_pid}
        end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:durable_version] ->
          {:ok, %{durable_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.metadata_materializer == materializer_pid
          assert updated_attempt.shard_layout
          assert updated_attempt.shard_materializers == %{0 => materializer_pid, 1 => user_materializer_pid}
        end)

      assert log =~ "Materializer caught up to version"
    end

    test "existing cluster recovers materializers for every shard in the recovered layout" do
      system_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      shard_layout = %{
        <<0xFF>> => {1, <<>>},
        Bedrock.end_of_keyspace() => {0, <<0xFF>>}
      }

      logs = %{
        "system_log" => [0],
        "user_log" => [1]
      }

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, logs)
        |> Map.put(:durable_version, durable_version)

      unlocks = :ets.new(:existing_cluster_materializer_unlocks, [:bag, :public])

      context =
        [
          old_transaction_system_layout: %{logs: logs}
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "mat_sys_0" => {:materializer, {:system_materializer, node()}, 0},
          "mat_user_1" => {:materializer, {:user_materializer, node()}, 1}
        })
        |> Map.put(:lock_materializer_fn, fn
          {:materializer, {:system_materializer, _node}, 0}, _epoch -> {:ok, system_materializer_pid}
          {:materializer, {:user_materializer, _node}, 1}, _epoch -> {:ok, user_materializer_pid}
        end)
        |> Map.put(:unlock_materializer_fn, fn pid, _version, tsl ->
          :ets.insert(unlocks, {:unlock, pid, Map.keys(tsl.logs)})
          :ok
        end)
        |> Map.put(:materializer_info_fn, fn _pid, [:durable_version] ->
          {:ok, %{durable_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn ^system_materializer_pid, _version -> {:ok, shard_layout} end)

      assert {updated_attempt, CommitProxyStartupPhase} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)

      assert updated_attempt.metadata_materializer == system_materializer_pid
      assert updated_attempt.shard_layout == shard_layout

      assert updated_attempt.shard_materializers == %{
               0 => system_materializer_pid,
               1 => user_materializer_pid
             }

      assert {:unlock, system_materializer_pid, ["system_log"]} in :ets.lookup(unlocks, :unlock)
      assert {:unlock, user_materializer_pid, ["user_log"]} in :ets.lookup(unlocks, :unlock)

      :ets.delete(unlocks)
    end

    test "creates new materializer when not found but capable nodes exist" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          {:ok, :new_materializer_ref}
        end)
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:durable_version] ->
          {:ok, %{durable_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.metadata_materializer == materializer_pid
          assert updated_attempt.shard_layout
        end)

      assert log =~ "System shard materializer not found, creating new one"
      assert log =~ "Materializer caught up to version"
    end

    test "stalls on catchup timeout" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)
      # Materializer reports lower version, never catches up
      materializer_version = Version.from_integer(50)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:materializer, {:test_materializer, node()}}
        })
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:durable_version] ->
          {:ok, %{durable_version: materializer_version}}
        end)
        # Use very short timeout for testing
        |> Map.put(:catchup_timeout_ms, 50)
        |> Map.put(:catchup_poll_interval_ms, 10)

      log =
        capture_log(fn ->
          assert {_attempt, {:stalled, :catchup_timeout}} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)
        end)

      # Should log waiting messages before timing out
      assert log =~ "Materializer at version"
      assert log =~ "waiting for"
    end

    test "stalls on unlock failure" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:materializer, {:test_materializer, node()}}
        })
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl ->
          {:error, :test_unlock_error}
        end)

      # No logs expected - unlock fails immediately
      assert {_attempt, {:stalled, {:unlock_failed, :test_unlock_error}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end

    test "stalls on worker creation failure" do
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          {:error, :foreman_unavailable}
        end)

      log =
        capture_log(fn ->
          {_attempt, {:stalled, result}} = MaterializerBootstrapPhase.execute(recovery_attempt, context)
          assert {:failed_to_create_materializer, :foreman_unavailable, 0} = result
        end)

      assert log =~ "System shard materializer not found, creating new one"
    end

    test "filters logs to only system shard when unlocking" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      # Logs with different shard assignments
      # log_1 handles shard 0 (system), log_2 handles shard 1 (user)
      logs = %{
        "log_1" => [0],
        "log_2" => [1]
      }

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, logs)
        |> Map.put(:durable_version, durable_version)

      received_tsl = :ets.new(:test_tsl, [:bag, :public])

      context =
        [
          old_transaction_system_layout: %{
            logs: logs
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:materializer, {:test_materializer, node()}},
          "mat_user_1" => {:materializer, {:user_materializer, node()}, 1}
        })
        |> Map.put(:lock_materializer_fn, fn
          {:materializer, {:test_materializer, _node}}, _epoch -> {:ok, materializer_pid}
          {:materializer, {:user_materializer, _node}, 1}, _epoch -> {:ok, user_materializer_pid}
        end)
        |> Map.put(:unlock_materializer_fn, fn pid, _version, tsl ->
          :ets.insert(received_tsl, {:tsl, pid, tsl})
          :ok
        end)
        |> Map.put(:materializer_info_fn, fn _pid, [:durable_version] ->
          {:ok, %{durable_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {_updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          # Verify that only system shard logs were passed to unlock
          unlocks = :ets.lookup(received_tsl, :tsl)

          assert Enum.any?(unlocks, fn
                   {:tsl, ^materializer_pid, %{logs: %{"log_1" => [0]}}} -> true
                   _ -> false
                 end)

          assert Enum.any?(unlocks, fn
                   {:tsl, ^user_materializer_pid, %{logs: %{"log_2" => [1]}}} -> true
                   _ -> false
                 end)
        end)

      assert log =~ "Materializer caught up to version"

      :ets.delete(received_tsl)
    end
  end

  describe "default_shard_layout/0" do
    test "returns two shards: system and user" do
      layout = MaterializerBootstrapPhase.default_shard_layout()

      assert is_map(layout)
      assert map_size(layout) == 2

      # System shard: "" to 0xFF (tag 1)
      # User shard: 0xFF to end_of_keyspace (tag 0)
      assert Map.has_key?(layout, <<0xFF>>)
      assert Map.has_key?(layout, Bedrock.end_of_keyspace())
    end
  end

  describe "system_shard_id/0" do
    test "returns 0 for the system shard" do
      assert RecoveryAttempt.system_shard_id() == 0
    end
  end
end
