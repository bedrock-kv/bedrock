defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase
  alias Bedrock.DataPlane.CommitProxy.Metadata
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Key
  alias Bedrock.KeyRange
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  # Shared test data setup
  defp mock_transaction_system_layout do
    %{
      id: "test_layout_id",
      epoch: 1,
      director: self(),
      sequencer: self(),
      rate_keeper: nil,
      proxies: [self()],
      resolvers: [{<<0>>, self()}],
      logs: %{"log_1" => [1, 2]},
      services: %{
        "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
      },
      shard_layout: %{
        <<0xFF>> => {1, <<>>},
        <<0xFF, 0xFF>> => {0, <<0xFF>>}
      }
    }
  end

  defp base_recovery_attempt do
    layout = mock_transaction_system_layout()

    recovery_attempt()
    |> with_sequencer(self())
    |> with_proxies([self()])
    |> with_resolvers([{<<0>>, self()}])
    |> with_logs(%{"log_1" => [1, 2]})
    |> with_transaction_services(%{
      "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
    })
    |> Map.put(:transaction_system_layout, layout)
  end

  describe "execute/2" do
    test "succeeds with existing transaction system layout and transitions to completed" do
      expected_layout = mock_transaction_system_layout()
      recovery_attempt = base_recovery_attempt()

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

      # Services are sanitized: no live pid survives into durable storage.
      assert %{"log_1" => %{status: :unknown}} = decoded[:layout_services]
      assert decoded[{:layout_log, "log_1"}] == [1, 2]
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
            SystemKeys.shards_prefix(),
            SystemKeys.layout_logs_prefix()
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
        |> Map.put(SystemKeys.shard(2), "stale-shard-metadata-2")
        |> Map.put(SystemKeys.shard(3), "stale-shard-metadata-3")
        |> Map.put(SystemKeys.shard(4), "stale-shard-metadata-4")
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

      # Stale shard metadata and layout_log entries are gone too.
      shard_tags = store |> range_read(SystemKeys.shards_prefix()) |> Enum.map(&elem(&1, 0))
      assert shard_tags == [SystemKeys.shard(0), SystemKeys.shard(1)]

      log_keys = store |> range_read(SystemKeys.layout_logs_prefix()) |> Enum.map(&elem(&1, 0))
      assert log_keys == [SystemKeys.layout_log("log_1")]
    end

    test "shrinking shard layout leaves exactly 2 entries visible to proxy Metadata" do
      mutations = captured_system_mutations(base_recovery_attempt())

      stale_writes = [
        {:set, SystemKeys.shard_key(<<0x40>>), Values.encode_shard_key_entry(2, <<>>)},
        {:set, SystemKeys.shard_key(<<0x80>>), Values.encode_shard_key_entry(3, <<0x40>>)},
        {:set, SystemKeys.shard_key(<<0xFF, 0xFF>>), Values.encode_shard_key_entry(4, <<0x80>>)}
      ]

      {metadata, _stats} =
        Metadata.apply_updates(Metadata.new(), [
          {Bedrock.DataPlane.Version.from_integer(1), stale_writes},
          {Bedrock.DataPlane.Version.from_integer(2), mutations}
        ])

      assert metadata.shards == %{<<0xFF>> => 1, <<0xFF, 0xFF>> => 0}
      assert metadata.shard_metadata |> Map.keys() |> Enum.sort() == ["0", "1"]
    end

    test "shrinking shard layout leaves exactly 2 entries visible to RoutingData ETS" do
      mutations = captured_system_mutations(base_recovery_attempt())

      routing_data = RoutingData.new_empty()
      RoutingData.insert_shard(routing_data, <<0x40>>, 2)
      RoutingData.insert_shard(routing_data, <<0x80>>, 3)
      RoutingData.insert_shard(routing_data, <<0xFF, 0xFF>>, 4)

      updated = RoutingData.apply_mutations(routing_data, [{1, mutations}])

      assert :ets.tab2list(updated.shard_table) == [{<<0xFF>>, 1}, {<<0xFF, 0xFF>>, 0}]

      RoutingData.cleanup(routing_data)
    end

    test "cleared prefix ranges do not cover any other system-key family" do
      # shard_keys/ and shards/ are byte-prefix siblings (common prefix
      # "shard"); layout/logs/ neighbors layout/id and layout/services. The
      # strinc bound must keep each cleared range strictly within its own
      # family: '_' (0x5F) < 's' (0x73) puts strinc("...shard_keys/") =
      # "...shard_keys0" below every "...shards/" key.
      cleared_prefixes = [
        SystemKeys.shard_keys_prefix(),
        SystemKeys.shards_prefix(),
        SystemKeys.layout_logs_prefix()
      ]

      other_family_keys = [
        SystemKeys.shard_key(<<>>),
        SystemKeys.shard_key(<<0xFF, 0xFF>>),
        SystemKeys.shard("0"),
        SystemKeys.layout_log("log_1"),
        SystemKeys.layout_services(),
        SystemKeys.layout_id(),
        SystemKeys.materializer_key(<<>>),
        SystemKeys.materializer_key(<<0xFF, 0xFF>>)
      ]

      for prefix <- cleared_prefixes,
          key <- other_family_keys,
          not String.starts_with?(key, prefix) do
        range = KeyRange.from_prefix(prefix)

        refute KeyRange.contains?(range, key),
               "clear_range over #{inspect(prefix)} would clear foreign family key #{inspect(key)}"
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
