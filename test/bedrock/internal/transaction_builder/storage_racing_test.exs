defmodule Bedrock.Internal.TransactionBuilder.StorageRacingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.StorageRacing

  defp dummy_pid, do: spawn(fn -> Process.sleep(:infinity) end)

  # Two contiguous shards; tag 7 has no materializer, so keys in "m" ..< "z"
  # resolve to a segment with [] pids.
  defp partially_covered_state(covered_pid) do
    layout = %{
      shard_layout: %{"m" => {1, "a"}, "z" => {7, "m"}},
      metadata_materializer: nil,
      shard_materializers: %{1 => covered_pid}
    }

    %State{
      layout_index: LayoutIndex.build_index(layout),
      fastest_storage_servers: %{},
      read_version: Bedrock.DataPlane.Version.from_integer(1),
      fetch_timeout_in_ms: 50
    }
  end

  describe "race_storage_servers/3 with uncovered shards (bedrock-q67.1 regression)" do
    test "a key in a shard with no materializer fails loudly with layout_lookup_failed" do
      state = partially_covered_state(dummy_pid())

      operation_fn = fn _pid, _version, _timeout ->
        flunk("operation_fn must not be called for an uncovered shard")
      end

      # lookup_key! returns {range, []}; the previously-dead `{_key_range, []}`
      # branch raises, and the rescue converts it into a loud failure instead
      # of silently skipping the shard.
      assert {^state, {:failure, %{layout_lookup_failed: []}}} =
               StorageRacing.race_storage_servers(state, "q", operation_fn)
    end

    test "a key outside the keyspace also fails with layout_lookup_failed" do
      state = partially_covered_state(dummy_pid())

      operation_fn = fn _pid, _version, _timeout ->
        flunk("operation_fn must not be called for a key outside the keyspace")
      end

      assert {^state, {:failure, %{layout_lookup_failed: []}}} =
               StorageRacing.race_storage_servers(state, <<0xFF, 0xFE>>, operation_fn)
    end

    test "a key in a covered shard still races and returns the result" do
      covered = dummy_pid()
      state = partially_covered_state(covered)

      operation_fn = fn ^covered, _version, _timeout -> {:ok, :materialized_value} end

      assert {%State{} = updated_state, {:ok, {:materialized_value, {"a", "m"}}}} =
               StorageRacing.race_storage_servers(state, "banana", operation_fn)

      assert %{{"a", "m"} => ^covered} = updated_state.fastest_storage_servers
    end
  end
end
