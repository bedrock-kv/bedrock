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

          assert updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()] == system_materializer_pid

          # Every creation reaches transaction_services — the layout is
          # built from it, and reconciliation retires anything the layout
          # doesn't reference.
          created_pids =
            for {_id, %{kind: :materializer, status: {:up, pid}}} <- updated_attempt.transaction_services, do: pid

          assert system_materializer_pid in created_pids
          assert user_materializer_pid in created_pids
        end)

      assert log =~ "Fresh cluster detected"

      # Verify both shards were created
      shards = created_shards |> :ets.lookup(:shard) |> Enum.map(fn {:shard, tag} -> tag end)
      assert 0 in shards
      assert 1 in shards

      :ets.delete(created_shards)
    end

    test "for fresh cluster, seeds shard materializers with the epoch's pull sources" do
      system_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      log_1_pid = spawn(fn -> Process.sleep(:infinity) end)
      log_2_pid = spawn(fn -> Process.sleep(:infinity) end)

      logs = %{
        "log_1" => [],
        "log_2" => []
      }

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, logs)
        |> Map.put(:transaction_services, %{
          "log_1" => %{kind: :log, status: {:up, log_1_pid}},
          "log_2" => %{kind: :log, status: {:up, log_2_pid}}
        })

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
        |> Map.put(:unlock_materializer_fn, fn pid, _version, pull_sources ->
          :ets.insert(unlocks, {:unlock, pid, pull_sources})
          :ok
        end)

      assert {updated_attempt, CommitProxyStartupPhase} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)

      assert updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()] == system_materializer_pid

      # Replication spans all logs today, so every shard's replica set
      # covers both — each seed carries {log_id, ref} pairs, never a
      # cluster services map.
      expected = [{"log_1", log_1_pid}, {"log_2", log_2_pid}]

      for pid <- [system_materializer_pid, user_materializer_pid] do
        assert [{:unlock, ^pid, sources}] =
                 unlocks |> :ets.lookup(:unlock) |> Enum.filter(&match?({:unlock, ^pid, _}, &1))

        assert Enum.sort(sources) == expected
      end

      :ets.delete(unlocks)
    end

    test "stalls when no materializer capable nodes exist" do
      recovery_attempt =
        recovery_attempt()
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

    test "reuses the locked materializers on restart — one per shard, unlocked at the recovery version" do
      system_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_pid = spawn(fn -> Process.sleep(:infinity) end)
      recovery_version = Version.from_integer(24_000_000)
      test_pid = self()

      # The locking phase already locked both epoch-1 materializers and
      # collected their shard assignments; the durable floor has regressed
      # to zero (it is not persisted) — that must NOT become anyone's
      # rollback target.
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:version_vector, {Version.from_integer(0), recovery_version})
        |> Map.put(:durable_version, Version.from_integer(0))
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_sys" => %{kind: :materializer, shard_id: 0, durable_version: Version.from_integer(19_000_000)},
          "mat_user" => %{kind: :materializer, shard_id: 1, durable_version: Version.from_integer(19_000_000)}
        })
        |> Map.put(:transaction_services, %{
          "mat_sys" => %{kind: :materializer, status: {:up, system_pid}},
          "mat_user" => %{kind: :materializer, status: {:up, user_pid}}
        })

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
        |> Map.put(:lock_materializer_fn, fn {:materializer, ref}, _epoch -> {:ok, ref} end)
        |> Map.put(:unlock_materializer_fn, fn pid, version, _tsl ->
          send(test_pid, {:unlocked, pid, version})
          :ok
        end)
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: recovery_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {1, <<>>}, Bedrock.end_of_keyspace() => {0, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          # The system-shard survivor answers the layout query...
          assert updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()] == system_pid
          assert updated_attempt.shard_layout

          # ...and every shard in the layout gets its surviving
          # materializer — nothing newly created, nothing orphaned.
          assert updated_attempt.shard_materializers == %{0 => system_pid, 1 => user_pid}
        end)

      # Both were unlocked at the recovery version (vector last), never at
      # the regressed durable floor.
      assert_receive {:unlocked, ^system_pid, ^recovery_version}
      assert_receive {:unlocked, ^user_pid, ^recovery_version}

      assert log =~ "Materializer caught up to version"
    end

    test "when several materializers claim a shard, the most-advanced durable state wins" do
      real_pid = spawn(fn -> Process.sleep(:infinity) end)
      stray_pid = spawn(fn -> Process.sleep(:infinity) end)
      recovery_version = Version.from_integer(24_000_000)

      # A stray from an earlier failed recovery attempt: same shard, empty.
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:version_vector, {Version.from_integer(0), recovery_version})
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_real" => %{kind: :materializer, shard_id: 0, durable_version: Version.from_integer(19_000_000)},
          "mat_stray" => %{kind: :materializer, shard_id: 0, durable_version: Version.zero()}
        })
        |> Map.put(:transaction_services, %{
          "mat_real" => %{kind: :materializer, status: {:up, real_pid}},
          "mat_stray" => %{kind: :materializer, status: {:up, stray_pid}}
        })

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
        |> Map.put(:lock_materializer_fn, fn {:materializer, ref}, _epoch -> {:ok, ref} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: recovery_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{Bedrock.end_of_keyspace() => {0, <<0xFF>>}}}
        end)

      capture_log(fn ->
        assert {updated_attempt, CommitProxyStartupPhase} =
                 MaterializerBootstrapPhase.execute(recovery_attempt, context)

        assert updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()] == real_pid
        assert updated_attempt.shard_materializers == %{0 => real_pid}
      end)
    end

    test "creates new materializer when not found but capable nodes exist" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
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
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.shard_materializers[0] == materializer_pid
          assert updated_attempt.shard_layout

          # The creation is recorded: the layout will reference it, so
          # reconciliation cannot retire the worker recovery just made.
          assert Enum.any?(updated_attempt.transaction_services, fn
                   {_id, %{kind: :materializer, status: {:up, ^materializer_pid}}} -> true
                   _ -> false
                 end)
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
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:version_vector, {Version.from_integer(0), durable_version})
        |> Map.put(:durable_version, durable_version)
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_sys" => %{kind: :materializer, shard_id: 0, durable_version: durable_version}
        })
        |> Map.put(:transaction_services, %{
          "mat_sys" => %{kind: :materializer, status: {:up, materializer_pid}}
        })

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: materializer_version}}
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
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:version_vector, {Version.from_integer(0), durable_version})
        |> Map.put(:durable_version, durable_version)
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_sys" => %{kind: :materializer, shard_id: 0, durable_version: durable_version}
        })
        |> Map.put(:transaction_services, %{
          "mat_sys" => %{kind: :materializer, status: {:up, materializer_pid}}
        })

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
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

    test "seeds each unlocked materializer with its replica set of pull sources" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      log_1_pid = spawn(fn -> Process.sleep(:infinity) end)
      log_2_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      logs = %{
        "log_1" => [],
        "log_2" => []
      }

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, logs)
        |> Map.put(:version_vector, {Version.from_integer(0), durable_version})
        |> Map.put(:durable_version, durable_version)
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_sys" => %{kind: :materializer, shard_id: 0, durable_version: durable_version},
          "mat_user" => %{kind: :materializer, shard_id: 1, durable_version: durable_version}
        })
        |> Map.put(:transaction_services, %{
          "mat_sys" => %{kind: :materializer, status: {:up, materializer_pid}},
          "mat_user" => %{kind: :materializer, status: {:up, spawn(fn -> Process.sleep(:infinity) end)}},
          "log_1" => %{kind: :log, status: {:up, log_1_pid}},
          "log_2" => %{kind: :log, status: {:up, log_2_pid}}
        })

      received_sources = :ets.new(:test_sources, [:set, :public])

      context =
        [
          old_transaction_system_layout: %{
            logs: logs
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
        |> Map.put(:lock_materializer_fn, fn {:materializer, ref}, _epoch -> {:ok, ref} end)
        |> Map.put(:unlock_materializer_fn, fn pid, _version, pull_sources ->
          :ets.insert(received_sources, {pid, pull_sources})
          :ok
        end)
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {_updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          # The seed is the shard's replica set — {log_id, ref} pairs
          # resolved by the director, not a services map for the
          # materializer to re-derive placement from. Replication spans
          # all logs today, so the system shard's set covers both.
          assert [{_, sources}] = :ets.lookup(received_sources, materializer_pid)
          assert Enum.sort(sources) == [{"log_1", log_1_pid}, {"log_2", log_2_pid}]
        end)

      assert log =~ "Materializer caught up to version"

      :ets.delete(received_sources)
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

  describe "shard_layout_from_entries/1" do
    alias Bedrock.SystemKeys
    alias Bedrock.SystemKeys.Values

    test "decodes tuple-encoded shard values and rebuilds contiguous start keys" do
      entries = [
        {SystemKeys.shard_key(<<0xFF, 0xFF>>), Values.encode_shard_key_entry(0, "m")},
        {SystemKeys.shard_key("m"), Values.encode_shard_key_entry(1, "")}
      ]

      assert {:ok, %{"m" => {1, ""}, <<0xFF, 0xFF>> => {0, "m"}}} =
               MaterializerBootstrapPhase.shard_layout_from_entries(entries)
    end

    test "decodes legacy term_to_binary shard values in both historical shapes" do
      entries = [
        {SystemKeys.shard_key("m"), :erlang.term_to_binary(1)},
        {SystemKeys.shard_key(<<0xFF, 0xFF>>), :erlang.term_to_binary({0, "m"})}
      ]

      assert {:ok, %{"m" => {1, ""}, <<0xFF, 0xFF>> => {0, "m"}}} =
               MaterializerBootstrapPhase.shard_layout_from_entries(entries)
    end

    test "rejects values that decode in neither encoding" do
      key = SystemKeys.shard_key("m")

      assert {:error, {:invalid_shard_value, ^key}} =
               MaterializerBootstrapPhase.shard_layout_from_entries([{key, "garbage"}])
    end
  end
end
