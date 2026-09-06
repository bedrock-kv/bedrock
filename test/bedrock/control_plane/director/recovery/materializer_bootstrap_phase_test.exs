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
          prior_core_state: %{logs: %{}},
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

          # Only the system shard is recovery's to create: the data
          # shard's slot stays ABSENT for the distributor to cover with
          # the placeholder and heal by recruitment (stall-only-for-
          # tag-0 completed).
          assert map_size(updated_attempt.shard_materializers) == 1
          assert Map.has_key?(updated_attempt.shard_materializers, 0)
          refute Map.has_key?(updated_attempt.shard_materializers, 1)

          assert [{<<_::binary>>, node_string}] =
                   Map.to_list(updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()])

          assert node_string == Atom.to_string(node())

          # Every creation reaches transaction_services — the layout is
          # built from it, and reconciliation retires anything the layout
          # doesn't reference.
          created_pids =
            for {_id, %{kind: :materializer, status: {:up, pid}}} <- updated_attempt.transaction_services, do: pid

          assert system_materializer_pid in created_pids
          refute user_materializer_pid in created_pids
        end)

      assert log =~ "Fresh cluster detected"

      # Only the system shard's materializer was created.
      shards = created_shards |> :ets.lookup(:shard) |> Enum.map(fn {:shard, tag} -> tag end)
      assert shards == [0]

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
          prior_core_state: %{logs: %{}},
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

      assert [{<<_::binary>>, <<_::binary>>}] =
               Map.to_list(updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()])

      # Replication spans all logs today, so the system shard's replica
      # set covers both — the seed carries {log_id, ref} pairs, never a
      # cluster services map. (The data shard is not created; its seed
      # is the distributor's, at recruitment.)
      expected = [{"log_1", log_1_pid}, {"log_2", log_2_pid}]

      assert [{:unlock, ^system_materializer_pid, sources}] =
               unlocks
               |> :ets.lookup(:unlock)
               |> Enum.filter(&match?({:unlock, ^system_materializer_pid, _}, &1))

      assert Enum.sort(sources) == expected

      assert [] = unlocks |> :ets.lookup(:unlock) |> Enum.filter(&match?({:unlock, ^user_materializer_pid, _}, &1))

      :ets.delete(unlocks)
    end

    test "a FRESH cluster stalls when no materializer capable nodes exist" do
      # Creation is legitimate here and nowhere else: a fresh cluster has
      # no prior data, so seeding the system shard invents nothing. (An
      # EXISTING cluster stalls on :no_system_materializers instead — it
      # must never manufacture the store its metadata lives in.)
      recovery_attempt = Map.put(recovery_attempt(), :shard_layout, nil)

      context =
        [
          prior_core_state: %{logs: %{}},
          node_capabilities: %{
            log: [Node.self()],
            storage: [Node.self()]
            # Note: no :materializer capability
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})

      capture_log(fn ->
        assert {_attempt, {:stalled, {:materializer_creation_failed, _}}} =
                 MaterializerBootstrapPhase.execute(recovery_attempt, context)
      end)
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
          prior_core_state: %{
            logs: %{"log_1" => [0, 1]},
            system_materializers: %{"mat_sys" => "node@host"}
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
          assert %{"mat_sys" => _node} = updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()]
          assert updated_attempt.shard_layout

          # ...and every shard in the layout gets its surviving
          # materializer — nothing newly created, nothing orphaned.
          assert %{0 => %{"mat_sys" => _}, 1 => %{"mat_user" => _}} = updated_attempt.shard_materializers

          assert map_size(updated_attempt.shard_materializers) == 2
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
          prior_core_state: %{
            logs: %{"log_1" => [0, 1]},
            system_materializers: %{"mat_real" => "node@host", "mat_stray" => "node@host"}
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

        assert %{"mat_real" => _node} = updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()]
        assert %{0 => %{"mat_real" => _}} = updated_attempt.shard_materializers
        assert map_size(updated_attempt.shard_materializers) == 1
      end)
    end

    test "an existing cluster does NOT create a system materializer, even with capacity to spare" do
      # Available capacity is not a licence to invent. Recovery
      # manufacturing the store its own metadata lives in would come up
      # "successfully" on an empty shard layout and orphan the cluster's
      # data. FDB is unambiguous here: it locks exactly the servers its
      # coordinated state names and waits for a quorum of THOSE
      # (TagPartitionedLogSystem.actor.cpp:2549-2585), never substituting
      # another and never fabricating one.
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, Version.from_integer(100))

      context =
        [
          prior_core_state: %{
            logs: %{"log_1" => [0, 1]},
            system_materializers: %{"mat_sys" => "node@host"}
          },
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        # The named member is not among the services this epoch locked.
        |> Map.put(:available_services, %{})
        |> Map.put(:create_worker_fn, fn _f, _i, :materializer, _o ->
          flunk("recovery invented a system materializer instead of stalling")
        end)

      assert {_attempt, {:stalled, {:system_materializers_unavailable, %{"mat_sys" => "node@host"}}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
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
          prior_core_state: %{
            logs: %{"log_1" => [0, 1]},
            system_materializers: %{"mat_sys" => "node@host"}
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
          prior_core_state: %{
            logs: %{"log_1" => [0, 1]},
            system_materializers: %{"mat_sys" => "node@host"}
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

    test "a FRESH cluster stalls on worker creation failure" do
      # Seeding tag 0 is the one creation recovery still performs, so it
      # is the only place a creation failure can stall it.
      recovery_attempt = Map.put(recovery_attempt(), :shard_layout, nil)

      context =
        [
          prior_core_state: %{logs: %{}},
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

      capture_log(fn ->
        {_attempt, {:stalled, result}} = MaterializerBootstrapPhase.execute(recovery_attempt, context)
        assert {:materializer_creation_failed, {0, {:failed_to_create_materializer, :foreman_unavailable, 0}}} = result
      end)
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
          prior_core_state: %{
            logs: logs,
            system_materializers: %{"mat_sys" => "node@host"}
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

  describe "prior materializer family as re-adoption input" do
    defp existing_context(recovery_version, overrides) do
      base =
        [
          prior_core_state: %{
            logs: %{"log_1" => [0, 1]},
            system_materializers: %{"mat_sys" => "node@host"}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
        |> Map.put(:lock_materializer_fn, fn {:materializer, ref}, _epoch -> {:ok, ref} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _sources -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: recovery_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {1, <<>>}, Bedrock.end_of_keyspace() => {0, <<0xFF>>}}}
        end)

      Map.merge(base, overrides)
    end

    defp two_claimants_attempt(recovery_version, named_pid, stray_pid, sys_pid) do
      recovery_attempt()
      |> Map.put(:shard_layout, nil)
      |> Map.put(:logs, %{"log_1" => [0, 1]})
      |> Map.put(:version_vector, {Version.from_integer(0), recovery_version})
      |> Map.put(:materializer_recovery_info_by_id, %{
        "mat_sys" => %{kind: :materializer, shard_id: 0, durable_version: recovery_version},
        "mat_named" => %{kind: :materializer, shard_id: 1, durable_version: Version.from_integer(1)},
        "mat_stray" => %{kind: :materializer, shard_id: 1, durable_version: recovery_version}
      })
      |> Map.put(:transaction_services, %{
        "mat_sys" => %{kind: :materializer, status: {:up, sys_pid}},
        "mat_named" => %{kind: :materializer, status: {:up, named_pid}},
        "mat_stray" => %{kind: :materializer, status: {:up, stray_pid}},
        "log_1" => %{kind: :log, status: {:up, self()}}
      })
    end

    test "the family-named locked survivor beats a stray" do
      # The committed assignment is the authority; the deterministic pick
      # is only the fallback for tags the family does not name.
      recovery_version = Version.from_integer(500)
      sys_pid = spawn(fn -> Process.sleep(:infinity) end)
      named_pid = spawn(fn -> Process.sleep(:infinity) end)
      stray_pid = spawn(fn -> Process.sleep(:infinity) end)

      context =
        existing_context(recovery_version, %{
          read_prior_refs_fn: fn _pid, _version ->
            {:ok, %{1 => %{"mat_named" => Atom.to_string(node())}}}
          end
        })

      recovery_attempt = two_claimants_attempt(recovery_version, named_pid, stray_pid, sys_pid)

      ExUnit.CaptureLog.capture_log(fn ->
        assert {updated_attempt, CommitProxyStartupPhase} =
                 MaterializerBootstrapPhase.execute(recovery_attempt, context)

        assert %{"mat_named" => _node} = updated_attempt.shard_materializers[1]
        assert updated_attempt.prior_materializer_refs == %{1 => %{"mat_named" => Atom.to_string(node())}}
        refute updated_attempt.seeded_layout?
      end)
    end

    defp three_claimants_attempt(recovery_version, sys_pid, a_pid, b_pid, c_pid) do
      # THREE claimants, with id order deliberately disagreeing with BOTH
      # durable orders. Two candidates cannot discriminate: whichever way
      # you arrange them, the id rule agrees with either min-durable or
      # max-durable, so the test passes under a rule it was meant to
      # reject. Here min-id is mat_a, min-durable is mat_b, and
      # max-durable is mat_c — only the id rule produces mat_a.
      recovery_attempt()
      |> Map.put(:shard_layout, nil)
      |> Map.put(:logs, %{"log_1" => [0, 1]})
      |> Map.put(:version_vector, {Version.from_integer(0), recovery_version})
      |> Map.put(:materializer_recovery_info_by_id, %{
        "mat_sys" => %{kind: :materializer, shard_id: 0, durable_version: recovery_version},
        "mat_a" => %{kind: :materializer, shard_id: 1, durable_version: Version.from_integer(50)},
        "mat_b" => %{kind: :materializer, shard_id: 1, durable_version: Version.from_integer(1)},
        "mat_c" => %{kind: :materializer, shard_id: 1, durable_version: recovery_version}
      })
      |> Map.put(:transaction_services, %{
        "mat_sys" => %{kind: :materializer, status: {:up, sys_pid}},
        "mat_a" => %{kind: :materializer, status: {:up, a_pid}},
        "mat_b" => %{kind: :materializer, status: {:up, b_pid}},
        "mat_c" => %{kind: :materializer, status: {:up, c_pid}},
        "log_1" => %{kind: :log, status: {:up, self()}}
      })
    end

    test "a family entry naming an unlocked worker falls back to the LOWEST ID, not to any durable ranking" do
      recovery_version = Version.from_integer(500)
      sys_pid = spawn(fn -> Process.sleep(:infinity) end)
      named_pid = spawn(fn -> Process.sleep(:infinity) end)
      stray_pid = spawn(fn -> Process.sleep(:infinity) end)

      context =
        existing_context(recovery_version, %{
          read_prior_refs_fn: fn _pid, _version ->
            {:ok, %{1 => %{"mat_gone" => Atom.to_string(node())}}}
          end
        })

      recovery_attempt =
        three_claimants_attempt(
          recovery_version,
          sys_pid,
          named_pid,
          stray_pid,
          spawn(fn -> Process.sleep(:infinity) end)
        )

      ExUnit.CaptureLog.capture_log(fn ->
        assert {updated_attempt, CommitProxyStartupPhase} =
                 MaterializerBootstrapPhase.execute(recovery_attempt, context)

        # "mat_gone" was not locked this epoch, so the fallback decides —
        # and it decides by worker id, the same deterministic rule the
        # client-facing pick uses. Neither durable ranking picks mat_a:
        # durable_version measures stream POSITION, not completeness, so
        # ranking by it could seat a worker that merely pulled the log
        # tail over one holding the data.
        assert %{"mat_a" => _node} = updated_attempt.shard_materializers[1]
      end)
    end

    test "the committed config family rides the attempt to the phases that size the system" do
      # Read at the same version, from the same materializer, as the other
      # two durable families — FDB builds its DatabaseConfiguration from
      # exactly this read (ClusterRecovery.actor.cpp:1191), never from the
      # coordinators.
      recovery_version = Version.from_integer(500)
      sys_pid = spawn(fn -> Process.sleep(:infinity) end)
      named_pid = spawn(fn -> Process.sleep(:infinity) end)
      stray_pid = spawn(fn -> Process.sleep(:infinity) end)

      context =
        existing_context(recovery_version, %{
          read_committed_parameters_fn: fn _pid, version ->
            assert version == recovery_version
            {:ok, %{Bedrock.SystemKeys.desired_commit_proxies() => 4}}
          end
        })

      recovery_attempt = two_claimants_attempt(recovery_version, named_pid, stray_pid, sys_pid)

      ExUnit.CaptureLog.capture_log(fn ->
        assert {updated_attempt, CommitProxyStartupPhase} =
                 MaterializerBootstrapPhase.execute(recovery_attempt, context)

        assert updated_attempt.committed_parameters == %{Bedrock.SystemKeys.desired_commit_proxies() => 4}
      end)
    end

    test "a failed config read stalls the attempt — never a silent fall back to the anchor" do
      # Reading the family as empty would mean "not configured", and the
      # persistence phase would then seed the coordinator's anchor over a
      # cluster that has a committed configuration it could not read.
      recovery_version = Version.from_integer(500)
      sys_pid = spawn(fn -> Process.sleep(:infinity) end)
      named_pid = spawn(fn -> Process.sleep(:infinity) end)
      stray_pid = spawn(fn -> Process.sleep(:infinity) end)

      context =
        existing_context(recovery_version, %{
          read_committed_parameters_fn: fn _pid, _version -> {:error, {:config_query_failed, :timeout}} end
        })

      recovery_attempt = two_claimants_attempt(recovery_version, named_pid, stray_pid, sys_pid)

      assert {_attempt, {:stalled, {:config_query_failed, :timeout}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end

    test "a failed family read stalls the attempt — never a silently unnamed layout" do
      recovery_version = Version.from_integer(500)
      sys_pid = spawn(fn -> Process.sleep(:infinity) end)
      named_pid = spawn(fn -> Process.sleep(:infinity) end)
      stray_pid = spawn(fn -> Process.sleep(:infinity) end)

      context =
        existing_context(recovery_version, %{
          read_prior_refs_fn: fn _pid, _version -> {:error, {:prior_refs_query_failed, :timeout}} end
        })

      recovery_attempt = two_claimants_attempt(recovery_version, named_pid, stray_pid, sys_pid)

      assert {_attempt, {:stalled, {:prior_refs_query_failed, :timeout}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end

    test "the fresh path marks the layout seeded with an empty prior family" do
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})

      context =
        [
          prior_core_state: %{logs: %{}},
          node_capabilities: %{log: [Node.self()], materializer: [Node.self()]}
        ]
        |> create_test_context()
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          {:ok, :new_materializer_ref}
        end)
        |> Map.put(:lock_materializer_fn, fn {:materializer, _ref, _shard_tag}, _epoch ->
          {:ok, spawn(fn -> Process.sleep(:infinity) end)}
        end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _sources -> :ok end)

      ExUnit.CaptureLog.capture_log(fn ->
        assert {updated_attempt, CommitProxyStartupPhase} =
                 MaterializerBootstrapPhase.execute(recovery_attempt, context)

        assert updated_attempt.seeded_layout?
        assert updated_attempt.prior_materializer_refs == %{}
      end)
    end
  end

  describe "decode_prior_refs/1" do
    alias Bedrock.SystemKeys, as: SK
    alias Bedrock.SystemKeys.Values, as: V

    test "decodes members into per-tag sets and rejects foreign or undecodable entries" do
      entries = [
        {SK.materializer_key(0, "wkr_sys"), V.encode_materializer_node("n@h")},
        {SK.materializer_key(7, "wkr_a"), V.encode_materializer_node("n@h")},
        {SK.materializer_key(7, "wkr_b"), V.encode_materializer_node("n2@h")}
      ]

      # A tag's members are a set: two entries under tag 7 are two
      # members, not a last-writer-wins overwrite.
      assert {:ok, %{0 => %{"wkr_sys" => "n@h"}, 7 => %{"wkr_a" => "n@h", "wkr_b" => "n2@h"}}} =
               MaterializerBootstrapPhase.decode_prior_refs(entries)

      bad_key = SK.shard_key("m")

      assert {:error, {:invalid_materializer_entry, ^bad_key}} =
               MaterializerBootstrapPhase.decode_prior_refs([{bad_key, "x"}])

      garbage = SK.materializer_key(1, "wkr_a")

      assert {:error, {:invalid_materializer_entry, ^garbage}} =
               MaterializerBootstrapPhase.decode_prior_refs([{garbage, <<0xEE>>}])
    end
  end

  describe "read_all_shard_entries/1" do
    alias Bedrock.SystemKeys, as: Keys

    # A scripted range read keyed by the start key it expects: paging must
    # resume each page exactly after the last returned key.
    defp scripted(script), do: fn start_key -> Map.fetch!(script, start_key) end

    test "a single page under the limit passes through" do
      entries = [{Keys.shard_key("m"), "v1"}]
      script = %{Keys.shard_keys_prefix() => {:ok, {entries, false}}}

      assert {:ok, ^entries} = MaterializerBootstrapPhase.read_all_shard_entries(scripted(script))
    end

    test "pages through the continuation until the read reports no more" do
      page1 = [{Keys.shard_key("b"), "v1"}, {Keys.shard_key("f"), "v2"}]
      page2 = [{Keys.shard_key("m"), "v3"}]
      page3 = [{Keys.shard_key(<<0xFF, 0xFF>>), "v4"}]

      script = %{
        Keys.shard_keys_prefix() => {:ok, {page1, true}},
        Bedrock.Key.key_after(Keys.shard_key("f")) => {:ok, {page2, true}},
        Bedrock.Key.key_after(Keys.shard_key("m")) => {:ok, {page3, false}}
      }

      assert {:ok, entries} = MaterializerBootstrapPhase.read_all_shard_entries(scripted(script))
      assert entries == page1 ++ page2 ++ page3
    end

    test "an empty layout is an empty list, not an error" do
      script = %{Keys.shard_keys_prefix() => {:ok, {[], false}}}

      assert {:ok, []} = MaterializerBootstrapPhase.read_all_shard_entries(scripted(script))
    end

    test "a mid-continuation failure surfaces as a query failure — never a truncated layout" do
      page1 = [{Keys.shard_key("b"), "v1"}]

      script = %{
        Keys.shard_keys_prefix() => {:ok, {page1, true}},
        Bedrock.Key.key_after(Keys.shard_key("b")) => {:failure, :timeout, :ref}
      }

      assert {:error, {:shard_layout_query_failed, :timeout}} =
               MaterializerBootstrapPhase.read_all_shard_entries(scripted(script))
    end

    test "an empty page claiming more is a broken contract, not an infinite loop" do
      script = %{Keys.shard_keys_prefix() => {:ok, {[], true}}}

      assert {:error, {:shard_layout_query_failed, :empty_continuation_page}} =
               MaterializerBootstrapPhase.read_all_shard_entries(scripted(script))
    end
  end

  describe "shard_layout_from_entries/1" do
    alias Bedrock.SystemKeys
    alias Bedrock.SystemKeys.Values

    test "decodes tuple-encoded shard values, consuming the carried start keys" do
      entries = [
        {SystemKeys.shard_key(<<0xFF, 0xFF>>), Values.encode_shard_key_entry(0, "m")},
        {SystemKeys.shard_key("m"), Values.encode_shard_key_entry(1, "")}
      ]

      assert {:ok, %{"m" => {1, ""}, <<0xFF, 0xFF>> => {0, "m"}}} =
               MaterializerBootstrapPhase.shard_layout_from_entries(entries)
    end

    test "the carried start key is consumed verbatim — no adjacency reconstruction" do
      # The value carries the fact; readers must not rebuild it. Under
      # adjacency reconstruction this entry's start would come out as the
      # empty key (the first shard "starts where nothing ended"), so a
      # carried non-empty start surviving proves the value is consumed —
      # the same meaning RoutingData.apply_mutation gives it.
      entries = [{SystemKeys.shard_key("m"), Values.encode_shard_key_entry(1, "gap")}]

      assert {:ok, %{"m" => {1, "gap"}}} =
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

    test "any legacy entry falls the whole snapshot back to adjacency" do
      # Structurally precluded in production (the family rewrites
      # atomically each epoch, so snapshots are encoding-uniform), but the
      # branch is live: a modern entry's carried start key is discarded
      # and its tag still extracted when a legacy sibling forces the
      # adjacency rebuild.
      entries = [
        {SystemKeys.shard_key("m"), Values.encode_shard_key_entry(1, "carried-start")},
        {SystemKeys.shard_key(<<0xFF, 0xFF>>), :erlang.term_to_binary(0)}
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

  describe "the system shard is looked up; discovery is the legacy migration only" do
    test "an existing cluster whose core state names no system materializer, and has none to adopt, STALLS" do
      # Recovery must never manufacture the store its own metadata lives
      # in. FDB refuses the same way: it builds its log system from
      # exactly the servers the coordinated state names
      # (TagPartitionedLogSystem.actor.cpp:2549-2585) and waits for a
      # quorum of THOSE rather than substituting others.
      recovery_attempt = recovery_attempt()

      context =
        recovery_context()
        |> Map.put(:prior_core_state, %{logs: %{"log_1" => [0]}, system_materializers: %{}})
        |> Map.put(:create_worker_fn, fn _f, _i, _k, _o ->
          flunk("recovery invented a system materializer instead of stalling")
        end)

      assert {_attempt, {:stalled, :no_system_materializer_found}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end

    test "a legacy record naming NO members adopts a locked tag-0 survivor instead of stalling forever" do
      # The upgrade path. A bootstrap written before system_materializers
      # existed names nobody, and treating that as unrecoverable bricks
      # every cluster created before the field: recovery stalls, the
      # director retries the stall, and every client read hangs.
      #
      # The information is not actually lost. The locking phase has
      # already locked every advertised materializer and each reports its
      # own shard_id, so tag 0 can be READ from evidence rather than
      # invented. Recovery then records what it adopted, and the next
      # recovery takes the named path — a one-time, self-healing
      # migration.
      system_pid = spawn(fn -> Process.sleep(:infinity) end)
      recovery_version = Version.from_integer(500)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0]})
        |> Map.put(:version_vector, {Version.from_integer(0), recovery_version})
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_legacy" => %{kind: :materializer, shard_id: 0, durable_version: recovery_version}
        })
        |> Map.put(:transaction_services, %{"mat_legacy" => %{kind: :materializer, status: {:up, system_pid}}})

      context =
        recovery_context()
        # The legacy shape: the field is simply absent from the record.
        |> Map.put(:prior_core_state, %{logs: %{"log_1" => [0]}})
        |> Map.put(:lock_materializer_fn, fn {:materializer, ref}, _epoch -> {:ok, ref} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _v, _s -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: recovery_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _v ->
          {:ok, %{Bedrock.end_of_keyspace() => {0, <<0xFF>>}}}
        end)
        |> Map.put(:create_worker_fn, fn _f, _i, _k, _o ->
          flunk("the migration invented a materializer instead of adopting the survivor")
        end)

      capture_log(fn ->
        assert {updated_attempt, CommitProxyStartupPhase} =
                 MaterializerBootstrapPhase.execute(recovery_attempt, context)

        # Recorded, so the persistence phase writes the field and the NEXT
        # recovery resolves by name. That is what makes it one-time.
        assert %{"mat_legacy" => _node} =
                 updated_attempt.shard_materializers[RecoveryAttempt.system_shard_id()]
      end)
    end

    test "a named system materializer that is not available STALLS rather than falling back" do
      recovery_attempt = recovery_attempt()

      context =
        recovery_context()
        |> Map.put(:prior_core_state, %{
          logs: %{"log_1" => [0]},
          system_materializers: %{"wkr_gone" => "dead@host"}
        })
        |> Map.put(:create_worker_fn, fn _f, _i, _k, _o ->
          flunk("recovery invented a replacement for an unavailable named member")
        end)

      # The reason carries the members and the nodes they were last seen
      # on: this one is retryable, and that is where to go looking.
      assert {_attempt, {:stalled, {:system_materializers_unavailable, %{"wkr_gone" => "dead@host"}}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end

    test "legacy discovery STALLS when more than one worker claims tag 0" do
      # The migration runs only on pre-q67.21.12 records — precisely the
      # clusters whose recovery could invent a replacement when a tag-0
      # node missed the 2s roll call. So empty strays claiming shard 0
      # are exactly the population here, and picking by lowest RANDOM
      # worker id would be a coin toss whose result is then written to
      # the durable record and resolved by name forever.
      #
      # Ambiguity is not a decision recovery gets to make silently.
      a = spawn(fn -> Process.sleep(:infinity) end)
      b = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_a" => %{kind: :materializer, shard_id: 0, durable_version: Version.from_integer(500)},
          "mat_b" => %{kind: :materializer, shard_id: 0, durable_version: Version.from_integer(500)}
        })
        |> Map.put(:transaction_services, %{
          "mat_a" => %{kind: :materializer, status: {:up, a}},
          "mat_b" => %{kind: :materializer, status: {:up, b}}
        })

      context = Map.put(recovery_context(), :prior_core_state, %{logs: %{"log_1" => [0]}})

      assert {_attempt, {:stalled, {:ambiguous_system_materializer, ["mat_a", "mat_b"]}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end

    test "a recovered shard layout that reads EMPTY stalls instead of completing" do
      # An empty shard_keys family decodes as a successful read of no
      # shards (Reader.shard_layout_from_entries([]) -> {:ok, %{}}), and
      # nothing downstream objects: resolvers for zero ranges is also
      # {:ok, []}. So recovery would COMPLETE on a cluster with no
      # keyspace map at all.
      #
      # A cluster with committed logs has a committed layout — the same
      # recovery writes both. So an empty read is never an empty cluster;
      # it is a materializer that cannot answer for the system shard, and
      # adopting its silence would orphan every shard the cluster owns.
      system_pid = spawn(fn -> Process.sleep(:infinity) end)
      recovery_version = Version.from_integer(500)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0]})
        |> Map.put(:version_vector, {Version.from_integer(0), recovery_version})
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_empty" => %{kind: :materializer, shard_id: 0, durable_version: recovery_version}
        })
        |> Map.put(:transaction_services, %{"mat_empty" => %{kind: :materializer, status: {:up, system_pid}}})

      context =
        recovery_context()
        |> Map.put(:prior_core_state, %{
          logs: %{"log_1" => [0]},
          system_materializers: %{"mat_empty" => "n@host"}
        })
        |> Map.put(:lock_materializer_fn, fn {:materializer, ref}, _epoch -> {:ok, ref} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _v, _s -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: recovery_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _v -> {:ok, %{}} end)

      capture_log(fn ->
        assert {_attempt, {:stalled, :recovered_shard_layout_is_empty}} =
                 MaterializerBootstrapPhase.execute(recovery_attempt, context)
      end)
    end

    test "a NAMED-but-unavailable record never falls back to a healthy stranger" do
      # The invariant the legacy migration must not weaken. Discovery is
      # for a record that names NOBODY; a record that names someone is
      # authoritative, and substituting a different worker is exactly the
      # fabrication FDB refuses. Here a perfectly healthy tag-0
      # materializer is locked and available — and recovery must still
      # stall, because the record does not name it.
      stranger_pid = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:materializer_recovery_info_by_id, %{
          "mat_stranger" => %{kind: :materializer, shard_id: 0, durable_version: Version.from_integer(500)}
        })
        |> Map.put(:transaction_services, %{"mat_stranger" => %{kind: :materializer, status: {:up, stranger_pid}}})

      context =
        Map.put(recovery_context(), :prior_core_state, %{
          logs: %{"log_1" => [0]},
          system_materializers: %{"wkr_gone" => "dead@host"}
        })

      assert {_attempt, {:stalled, {:system_materializers_unavailable, %{"wkr_gone" => "dead@host"}}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end
  end
end
