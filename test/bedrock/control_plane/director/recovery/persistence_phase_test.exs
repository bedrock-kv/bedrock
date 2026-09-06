defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.KeyRange
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  # Shared test data setup: the TSL is wiring only (no membership map);
  # shard topology and service records ride the recovery attempt (and
  # from there the keyspace).
  defp mock_transaction_system_layout do
    %{
      epoch: 1,
      sequencer: self(),
      proxies: [self()],
      resolvers: [{<<0>>, self()}],
      logs: %{"log_1" => [1, 2]}
    }
  end

  defp node_string, do: Atom.to_string(node())

  defp base_recovery_attempt do
    mat_sys = spawn(fn -> Process.sleep(:infinity) end)
    mat_user = spawn(fn -> Process.sleep(:infinity) end)

    recovery_attempt()
    |> with_sequencer(self())
    |> with_proxies([self()])
    |> with_resolvers([{<<0>>, self()}])
    |> with_logs(%{"log_1" => [1, 2]})
    |> with_transaction_services(%{
      "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
      "wkr_sys" => %{status: {:up, mat_sys}, kind: :materializer, last_seen: {:wkr_sys_name, node()}},
      "wkr_user" => %{status: {:up, mat_user}, kind: :materializer, last_seen: {:wkr_user_name, node()}}
    })
    |> Map.put(:shard_layout, %{
      <<0xFF>> => {1, <<>>},
      <<0xFF, 0xFF>> => {0, <<0xFF>>}
    })
    |> Map.put(:shard_materializers, %{0 => %{"wkr_sys" => node_string()}, 1 => %{"wkr_user" => node_string()}})
    |> Map.put(:seeded_layout?, true)
    |> Map.put(:prior_materializer_refs, %{})
    |> Map.put(:transaction_system_layout, mock_transaction_system_layout())
  end

  describe "execute/2" do
    test "succeeds with existing transaction system layout and transitions to completed" do
      recovery_attempt = base_recovery_attempt()
      expected_layout = recovery_attempt.transaction_system_layout

      context = Map.put(recovery_context(), :commit_transaction_fn, fn _, _, _ -> {:ok, 1, 0} end)

      # Pattern match both result and next phase in single assertion
      assert {%{transaction_system_layout: ^expected_layout}, :completed} =
               PersistencePhase.execute(recovery_attempt, context)
    end

    test "fails when system transaction fails" do
      recovery_attempt = base_recovery_attempt()
      context = Map.put(recovery_context(), :commit_transaction_fn, fn _, _, _ -> {:error, :timeout} end)

      # Pattern match tuple destructuring with expected stall reason
      assert {_, {:stalled, {:recovery_system_failed, :timeout}}} =
               PersistencePhase.execute(recovery_attempt, context)
    end

    test "every system-key value it writes decodes through Values.decode_for/2" do
      recovery_attempt = base_recovery_attempt()
      test_pid = self()

      commit_fn = fn _proxy, _epoch, encoded_transaction ->
        send(test_pid, {:committed, encoded_transaction})
        {:ok, 1, 0}
      end

      context = Map.put(recovery_context(), :commit_transaction_fn, commit_fn)
      assert {_, :completed} = PersistencePhase.execute(recovery_attempt, context)
      assert_received {:committed, encoded_transaction}

      sets =
        encoded_transaction
        |> Transaction.mutations!()
        |> Enum.flat_map(fn
          {:set, key, value} -> [{key, value}]
          _ -> []
        end)

      refute sets == []

      # Every written value must decode via the family dispatched from its key;
      # this is the writer/reader contract that masked the pre-fix shard_key bug.
      decoded =
        Map.new(sets, fn {key, value} ->
          parsed = SystemKeys.parse_key(key)
          refute parsed in [:unknown, :error], "wrote unparseable system key: #{inspect(key)}"
          assert {:ok, decoded} = Values.decode_for(parsed, value), "value for #{inspect(key)} failed to decode"
          {parsed, decoded}
        end)

      # Shard keys must decode to the exact {tag, start_key} the layout holds --
      # the shape the materializer bootstrap cross-epoch read depends on.
      assert decoded[{:shard_key, <<0xFF>>}] == {1, <<>>}
      assert decoded[{:shard_key, <<0xFF, 0xFF>>}] == {0, <<0xFF>>}

      # Membership: the worker id is in the KEY and the value carries the
      # node (FDB serverKeys analogue), projected from the carried
      # shard_materializers refs.
      node_string = Atom.to_string(node())
      assert decoded[{:materializer_key, 0, "wkr_sys"}] == node_string
      assert decoded[{:materializer_key, 1, "wkr_user"}] == node_string
    end

    test "materializer family is skipped entirely when shard_materializers is absent" do
      recovery_attempt = Map.put(base_recovery_attempt(), :shard_materializers, %{})
      mutations = captured_system_mutations(recovery_attempt)

      prefix = SystemKeys.materializers_prefix()
      {clear_start, clear_end} = KeyRange.from_prefix(prefix)

      refute Enum.any?(mutations, fn
               {:set, key, _} -> String.starts_with?(key, prefix)
               {:clear_range, ^clear_start, ^clear_end} -> true
               _ -> false
             end)
    end

    test "assignments write from the carried refs — no services-map inversion, no skips" do
      # Worker ids and nodes ride the assignment from creation, so the
      # family is written even when the services map has no matching
      # record: the keyspace names exactly what the attempt assigned.
      # (Under the old inversion, a missing record silently dropped the
      # entry — an orphan the seed and keyspace then disagreed about.)
      recovery_attempt =
        update_in(
          base_recovery_attempt(),
          [Access.key!(:transaction_services)],
          &Map.drop(&1, ["wkr_sys", "wkr_user"])
        )

      mutations = captured_system_mutations(recovery_attempt)

      for tag <- [0, 1] do
        assert Enum.any?(mutations, fn
                 {:set, key, _} -> String.starts_with?(key, SystemKeys.materializer_tag_prefix(tag))
                 _ -> false
               end)
      end
    end

    test "commits the system transaction in system mode by default" do
      # Without an injected commit fn, the phase must reach the proxy through
      # the system-mode commit path: user-mode commits cannot write \xFF keys.
      test_pid = self()

      stub_proxy =
        spawn_link(fn ->
          receive do
            {:"$gen_call", from, {:commit, epoch, encoded_transaction, mode}} ->
              send(test_pid, {:committed, epoch, encoded_transaction, mode})
              GenServer.reply(from, {:ok, 1, 0})
          end
        end)

      recovery_attempt =
        base_recovery_attempt()
        |> with_proxies([stub_proxy])
        |> put_in([Access.key!(:transaction_system_layout), :proxies], [stub_proxy])

      assert {_, :completed} = PersistencePhase.execute(recovery_attempt, recovery_context())
      assert_received {:committed, _epoch, encoded_transaction, :system}

      assert Enum.any?(Transaction.mutations!(encoded_transaction), fn
               {:set, <<0xFF, _::binary>>, _} -> true
               _ -> false
             end)
    end
  end

  # Capture the mutations of the system transaction PersistencePhase commits.
  defp captured_system_mutations(recovery_attempt) do
    test_pid = self()

    commit_fn = fn _proxy, _epoch, encoded_transaction ->
      send(test_pid, {:committed, encoded_transaction})
      {:ok, 1, 0}
    end

    context = Map.put(recovery_context(), :commit_transaction_fn, commit_fn)
    assert {_, :completed} = PersistencePhase.execute(recovery_attempt, context)
    assert_received {:committed, encoded_transaction}
    Transaction.mutations!(encoded_transaction)
  end

  # Apply set/clear/clear_range mutations, in order, to a flat key -> value map
  # (a stand-in for the materializer's durable key space).
  defp apply_to_store(store, mutations) do
    Enum.reduce(mutations, store, fn
      {:set, key, value}, store ->
        Map.put(store, key, value)

      {:clear, key}, store ->
        Map.delete(store, key)

      {:clear_range, start_key, end_key}, store ->
        Map.reject(store, fn {key, _} -> key >= start_key and key < end_key end)
    end)
  end

  # Range read over [prefix, strinc(prefix)) -- the same semantics as the
  # materializer bootstrap cross-epoch read (default_get_shard_layout).
  defp range_read(store, prefix) do
    {start_key, end_key} = KeyRange.from_prefix(prefix)

    store
    |> Enum.filter(fn {key, _} -> key >= start_key and key < end_key end)
    |> Enum.sort()
  end

  describe "read-and-heal: recovery writes what it changed, never blanket-clears" do
    test "recovery clears nothing: every family it writes is read-and-healed" do
      mutations = captured_system_mutations(base_recovery_attempt())

      # The epoch-scoped layout/logs family was the only thing recovery
      # ever blanket-cleared, and it had no reader (bedrock-q67.21.10).
      # What remains is durable, distributor-era state: recovery may seed
      # or update entries, never erase a family wholesale.
      refute Enum.any?(mutations, &match?({:clear_range, _, _}, &1)),
             "recovery emitted a blanket clear: #{inspect(Enum.filter(mutations, &match?({:clear_range, _, _}, &1)))}"

      refute Enum.any?(mutations, &match?({:clear, _}, &1))
    end

    test "a fresh cluster seeds shard_keys; the seed needs no clear (the family is definitionally empty)" do
      mutations = captured_system_mutations(base_recovery_attempt())

      shard_sets =
        Enum.filter(mutations, fn
          {:set, key, _} -> String.starts_with?(key, SystemKeys.shard_keys_prefix())
          _ -> false
        end)

      assert length(shard_sets) == 2
    end

    test "an existing cluster's recovery leaves the durable shard_keys family untouched" do
      # Boundaries never change without splits; the family was read, not
      # invented, so recovery writes nothing under it.
      recovery_attempt = Map.put(base_recovery_attempt(), :seeded_layout?, false)

      stale_store = %{
        SystemKeys.shard_key(<<0xFF>>) => Values.encode_shard_key_entry(1, <<>>),
        SystemKeys.shard_key(<<0xFF, 0xFF>>) => Values.encode_shard_key_entry(0, <<0xFF>>)
      }

      mutations = captured_system_mutations(recovery_attempt)
      store = apply_to_store(stale_store, mutations)

      refute Enum.any?(mutations, fn
               {:set, key, _} -> String.starts_with?(key, SystemKeys.shard_keys_prefix())
               _ -> false
             end)

      assert range_read(store, SystemKeys.shard_keys_prefix()) == Enum.sort(stale_store)
    end

    test "materializer writes are a diff against the prior family; unnamed entries are not recovery's to clean" do
      # tag 0's assignment is unchanged (not rewritten); tag 1's changed
      # (rewritten); tag 9's entry names a tag outside this layout and is
      # left alone — read-and-heal means stale entries are the
      # distributor's to reconcile, never recovery's to erase.
      prior = %{
        0 => %{"wkr_sys" => node_string()},
        1 => %{"wkr_departed" => node_string()},
        9 => %{"wkr_stray" => node_string()}
      }

      recovery_attempt = Map.put(base_recovery_attempt(), :prior_materializer_refs, prior)

      stale_store =
        Map.new(prior, fn {tag, members} ->
          [{id, node}] = Map.to_list(members)
          {SystemKeys.materializer_key(tag, id), Values.encode_materializer_node(node)}
        end)

      mutations = captured_system_mutations(recovery_attempt)
      store = apply_to_store(stale_store, mutations)

      materializer_sets =
        for {:set, key, value} <- mutations,
            String.starts_with?(key, SystemKeys.materializers_prefix()),
            do: {key, value}

      # tag 1's member changed, so its NEW member's key is written; the
      # departed member's key is left alone — recovery writes what it
      # decided and never removes members it did not place (a set may
      # legitimately hold replicas recovery knows nothing about).
      assert materializer_sets == [
               {SystemKeys.materializer_key(1, "wkr_user"), Values.encode_materializer_node(node_string())}
             ]

      assert {:ok, _node} =
               Values.decode_materializer_node(Map.fetch!(store, SystemKeys.materializer_key(0, "wkr_sys")))

      assert {:ok, _node} =
               Values.decode_materializer_node(Map.fetch!(store, SystemKeys.materializer_key(9, "wkr_stray")))
    end
  end

  describe "rewriting a bootstrap record that predates a field" do
    alias Bedrock.ControlPlane.Config.Parameters
    alias Bedrock.ControlPlane.Config.RecoveryAttempt
    alias Bedrock.SystemKeys.ClusterBootstrap

    test "a legacy record without system_materializers is rewritten, not crashed on" do
      # The real decoded shape, not a hand-built map: a record written
      # before the field existed comes back WITHOUT the key, and
      # %{record | key: ...} raises badkey for a key the map lacks. That
      # crashed the director in a tight retry loop on every cluster
      # created before bedrock-q67.21.12 — the ones an upgrade must
      # carry, not brick.
      legacy =
        %{
          cluster_id: "abc",
          epoch: 3,
          logs: [%{id: "log_1", otp_ref: nil, shard_tags: []}],
          coordinators: [%{node: "a@host"}],
          parameters: %{
            desired_logs: 1,
            desired_replication_factor: 1,
            desired_commit_proxies: 1,
            desired_coordinators: 1,
            desired_read_version_proxies: 1,
            ping_rate_in_hz: 10,
            retransmission_rate_in_hz: 20,
            transaction_window_in_ms: 5000,
            empty_transaction_timeout_ms: 1000
          },
          policies: %{allow_volunteer_nodes_to_join: true}
        }
        |> ClusterBootstrap.to_binary()
        |> ClusterBootstrap.read()
        |> then(fn {:ok, decoded} -> decoded end)

      refute Map.has_key?(legacy, :system_materializers)

      attempt =
        %RecoveryAttempt{cluster: nil, epoch: 4, attempt: 1}
        |> Map.put(:shard_materializers, %{0 => %{"mat_sys" => "a@host"}})
        |> Map.put(:prior_materializer_refs, %{})

      # The config the director holds always came through the
      # coordinator's build_parameters/2, which fills every parameter
      # the record did not carry.
      config = %{
        parameters:
          Map.put(
            legacy.parameters,
            :materializer_idle_timeout_ms,
            Parameters.default_materializer_idle_timeout_ms()
          ),
        policies: legacy.policies
      }

      updated =
        PersistencePhase.build_updated_bootstrap(legacy, attempt, config, %{logs: %{"log_1" => []}})

      assert updated.epoch == 4
      assert updated.system_materializers == [%{id: "mat_sys", node: "a@host"}]
      # Untouched fields survive the merge.
      assert updated.cluster_id == "abc"
      assert updated.coordinators == [%{node: "a@host"}]
    end

    # bedrock-q67.21.8: zero means "never spin down", and a flatbuffer
    # omits a scalar equal to its schema default — so with the implicit
    # default of 0 a disabled cluster would read back as "absent" and be
    # silently re-enabled by the coordinator's fallback on the next
    # restart. The schema's non-zero default is what keeps the two
    # distinguishable.
    test "a cluster with materializer spin-down disabled survives the bootstrap round trip" do
      parameters = %{
        desired_logs: 1,
        desired_replication_factor: 1,
        desired_commit_proxies: 1,
        desired_coordinators: 1,
        desired_read_version_proxies: 1,
        ping_rate_in_hz: 10,
        retransmission_rate_in_hz: 20,
        transaction_window_in_ms: 5000,
        empty_transaction_timeout_ms: 1000,
        materializer_idle_timeout_ms: 0
      }

      assert {:ok, decoded} =
               %{cluster_id: "abc", epoch: 3, parameters: parameters}
               |> ClusterBootstrap.to_binary()
               |> ClusterBootstrap.read()

      assert decoded.parameters.materializer_idle_timeout_ms == 0

      # A record written before the field existed still reads as absent,
      # which is what lets the coordinator supply the default.
      assert {:ok, legacy} =
               %{
                 cluster_id: "abc",
                 epoch: 3,
                 parameters: Map.delete(parameters, :materializer_idle_timeout_ms)
               }
               |> ClusterBootstrap.to_binary()
               |> ClusterBootstrap.read()

      refute Map.has_key?(legacy.parameters, :materializer_idle_timeout_ms)
    end
  end
end
