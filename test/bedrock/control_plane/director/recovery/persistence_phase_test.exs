defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Key
  alias Bedrock.KeyRange
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  # Shared test data setup: the TSL is wiring only; shard topology rides
  # the recovery attempt (and from there the keyspace).
  defp mock_transaction_system_layout(services) do
    %{
      epoch: 1,
      sequencer: self(),
      proxies: [self()],
      resolvers: [{<<0>>, self()}],
      logs: %{"log_1" => [1, 2]},
      services: services
    }
  end

  defp base_recovery_attempt do
    mat_sys = spawn(fn -> Process.sleep(:infinity) end)
    mat_user = spawn(fn -> Process.sleep(:infinity) end)

    services = %{
      "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
      "wkr_sys" => %{status: {:up, mat_sys}, kind: :materializer, last_seen: {:wkr_sys_name, node()}},
      "wkr_user" => %{status: {:up, mat_user}, kind: :materializer, last_seen: {:wkr_user_name, node()}}
    }

    recovery_attempt()
    |> with_sequencer(self())
    |> with_proxies([self()])
    |> with_resolvers([{<<0>>, self()}])
    |> with_logs(%{"log_1" => [1, 2]})
    |> with_transaction_services(%{
      "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
    })
    |> Map.put(:shard_layout, %{
      <<0xFF>> => {1, <<>>},
      <<0xFF, 0xFF>> => {0, <<0xFF>>}
    })
    |> Map.put(:shard_materializers, %{0 => mat_sys, 1 => mat_user})
    |> Map.put(:transaction_system_layout, mock_transaction_system_layout(services))
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

      assert decoded[{:layout_log, "log_1"}] == [1, 2]

      # Materializer refs: worker id + node as strings (FDB serverList
      # analogue), derived by inverting shard_materializers through services.
      node_string = Atom.to_string(node())
      assert decoded[{:materializer_key, 0}] == {"wkr_sys", node_string}
      assert decoded[{:materializer_key, 1}] == {"wkr_user", node_string}
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

    test "active shard management with zero matching service records still clears the family" do
      # Keyspace and seed must agree: the seed would be empty, so the
      # keyspace must end empty too - the clear fires even with no sets.
      recovery_attempt =
        update_in(
          base_recovery_attempt(),
          [Access.key!(:transaction_system_layout), :services],
          &Map.drop(&1, ["wkr_sys", "wkr_user"])
        )

      mutations = captured_system_mutations(recovery_attempt)

      prefix = SystemKeys.materializers_prefix()
      {clear_start, clear_end} = KeyRange.from_prefix(prefix)

      assert Enum.any?(mutations, &match?({:clear_range, ^clear_start, ^clear_end}, &1))

      refute Enum.any?(mutations, fn
               {:set, key, _} -> String.starts_with?(key, prefix)
               _ -> false
             end)
    end

    test "a materializer pid without a service record is skipped, not invented" do
      orphan = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt =
        update_in(base_recovery_attempt(), [Access.key!(:shard_materializers)], &Map.put(&1, 9, orphan))

      mutations = captured_system_mutations(recovery_attempt)

      refute Enum.any?(mutations, fn
               {:set, key, _} -> key == SystemKeys.materializer_key(9)
               _ -> false
             end)

      assert Enum.any?(mutations, fn
               {:set, key, _} -> key == SystemKeys.materializer_key(0)
               _ -> false
             end)
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

  describe "stale entry clearing on recovery rewrite" do
    test "clears each rewritten keyed family's prefix before writing its entries" do
      mutations = captured_system_mutations(base_recovery_attempt())

      for prefix <- [
            SystemKeys.shard_keys_prefix(),
            SystemKeys.layout_logs_prefix(),
            SystemKeys.materializers_prefix()
          ] do
        {clear_start, clear_end} = KeyRange.from_prefix(prefix)

        clear_index =
          Enum.find_index(mutations, fn
            {:clear_range, ^clear_start, ^clear_end} -> true
            _ -> false
          end)

        assert clear_index, "expected clear_range over #{inspect(prefix)}"

        first_set_index =
          Enum.find_index(mutations, fn
            {:set, key, _} -> String.starts_with?(key, prefix)
            _ -> false
          end)

        assert first_set_index, "expected set mutations under #{inspect(prefix)}"

        assert clear_index < first_set_index,
               "clear_range over #{inspect(prefix)} must precede its set mutations"
      end
    end

    test "shrinking shard layout (3 -> 2) leaves exactly 2 entries visible to the bootstrap range read" do
      # Previous epoch persisted 3 shards; the third ends at <<0x80>>.
      stale_store =
        %{}
        |> Map.put(SystemKeys.shard_key(<<0x40>>), Values.encode_shard_key_entry(2, <<>>))
        |> Map.put(SystemKeys.shard_key(<<0x80>>), Values.encode_shard_key_entry(3, <<0x40>>))
        |> Map.put(SystemKeys.shard_key(<<0xFF, 0xFF>>), Values.encode_shard_key_entry(4, <<0x80>>))
        |> Map.put(SystemKeys.layout_log("old_log"), Values.encode_tag_list([9]))

      # This epoch's layout has only 2 shards (see mock_transaction_system_layout/0).
      mutations = captured_system_mutations(base_recovery_attempt())
      store = apply_to_store(stale_store, mutations)

      # (a) Bootstrap cross-epoch read: exactly the 2 current shard_key entries.
      assert [
               {shard_key_1, _},
               {shard_key_2, _}
             ] = range_read(store, SystemKeys.shard_keys_prefix())

      assert shard_key_1 == SystemKeys.shard_key(<<0xFF>>)
      assert shard_key_2 == SystemKeys.shard_key(<<0xFF, 0xFF>>)

      # Stale layout_log entries are gone too.
      log_keys = store |> range_read(SystemKeys.layout_logs_prefix()) |> Enum.map(&elem(&1, 0))
      assert log_keys == [SystemKeys.layout_log("log_1")]
    end

    test "shrinking shard layout leaves exactly 2 entries visible to RoutingData" do
      mutations = captured_system_mutations(base_recovery_attempt())

      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_shard(<<0x40>>, 2, <<>>)
        |> RoutingData.insert_shard(<<0x80>>, 3, <<0x40>>)
        |> RoutingData.insert_shard(<<0xFF, 0xFF>>, 4, <<0x80>>)

      updated =
        RoutingData.apply_mutations(routing_data, [{Bedrock.DataPlane.Version.from_integer(1), mutations}])

      assert :gb_trees.to_list(updated.shards) == [
               {<<0xFF>>, {1, <<>>}},
               {<<0xFF, 0xFF>>, {0, <<0xFF>>}}
             ]
    end

    test "cleared prefix ranges do not cover any other system-key family" do
      # The strinc bound must keep each cleared range strictly within its own
      # family, including against future sibling families that share prefix
      # bytes (e.g. a "shards/" neighbor of "shard_keys/": '_' (0x5F) < 's'
      # (0x73) puts strinc("...shard_keys/") = "...shard_keys0" below it).
      cleared_prefixes = %{
        shard_key: SystemKeys.shard_keys_prefix(),
        layout_log: SystemKeys.layout_logs_prefix()
      }

      keys_by_family = %{
        shard_key: [SystemKeys.shard_key(<<>>), SystemKeys.shard_key(<<0xFF, 0xFF>>)],
        shard_sibling: ["\xff/system/shards/0"],
        layout_log: [SystemKeys.layout_log("log_1")],
        layout_sibling: ["\xff/system/layout/id", "\xff/system/layout/services"]
      }

      # Foreignness is decided by family identity, not by whether the key
      # happens to share the prefix's bytes — so an over-broad prefix
      # definition (e.g. layout_logs_prefix returning "layout/") fails here
      # instead of being filtered out of the comparison.
      for {cleared_family, prefix} <- cleared_prefixes,
          {family, keys} <- keys_by_family,
          family != cleared_family,
          key <- keys do
        range = KeyRange.from_prefix(prefix)

        refute KeyRange.contains?(range, key),
               "clear_range over #{inspect(prefix)} would clear #{family} key #{inspect(key)}"
      end
    end

    test "prefix range end is exact: a key at strinc(prefix) is not cleared" do
      prefix = SystemKeys.shard_keys_prefix()
      boundary_key = Key.strinc(prefix)

      # A neighbor key exactly at the exclusive range end must survive, as must
      # the key just below the prefix (the prefix itself minus the trailing "/").
      below_key = binary_part(prefix, 0, byte_size(prefix) - 1)

      store = %{boundary_key => "at-range-end", below_key => "below-range-start"}

      mutations = captured_system_mutations(base_recovery_attempt())
      store = apply_to_store(store, mutations)

      assert store[boundary_key] == "at-range-end"
      assert store[below_key] == "below-range-start"
    end
  end
end
