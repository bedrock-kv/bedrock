defmodule Bedrock.DataPlane.Demux.MutationSlicerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux.MutationSlicer
  alias Bedrock.DataPlane.Transaction

  @commit_version <<0, 0, 0, 0, 0, 0, 10, 0>>

  describe "slice/2" do
    test "slices transaction with single shard" do
      mutations = [
        {:set, "key1", "value1"},
        {:set, "key2", "value2"}
      ]

      # shard_index indicates all mutations belong to shard 0
      txn =
        Transaction.encode(%{
          mutations: mutations,
          shard_index: [{0, 2}]
        })

      assert {:ok, slices} = MutationSlicer.slice(txn, @commit_version)
      assert length(slices) == 1

      [{shard_id, slice}] = slices
      assert shard_id == 0

      # Verify the slice is a valid transaction with the correct mutations
      {:ok, decoded} = Transaction.decode(slice)
      assert decoded.mutations == mutations
      assert decoded.commit_version == @commit_version
    end

    test "slices transaction with multiple shards" do
      # Mutations for shard 0
      shard0_mutations = [
        {:set, "a_key1", "value1"},
        {:set, "a_key2", "value2"}
      ]

      # Mutations for shard 1
      shard1_mutations = [
        {:set, "m_key1", "value3"}
      ]

      # Mutations for shard 2
      shard2_mutations = [
        {:set, "z_key1", "value4"},
        {:set, "z_key2", "value5"},
        {:set, "z_key3", "value6"}
      ]

      all_mutations = shard0_mutations ++ shard1_mutations ++ shard2_mutations

      txn =
        Transaction.encode(%{
          mutations: all_mutations,
          shard_index: [{0, 2}, {1, 1}, {2, 3}]
        })

      assert {:ok, slices} = MutationSlicer.slice(txn, @commit_version)
      assert length(slices) == 3

      # Convert to map for easier testing
      slices_map = Map.new(slices)

      # Check shard 0
      {:ok, decoded0} = Transaction.decode(slices_map[0])
      assert decoded0.mutations == shard0_mutations
      assert decoded0.commit_version == @commit_version

      # Check shard 1
      {:ok, decoded1} = Transaction.decode(slices_map[1])
      assert decoded1.mutations == shard1_mutations
      assert decoded1.commit_version == @commit_version

      # Check shard 2
      {:ok, decoded2} = Transaction.decode(slices_map[2])
      assert decoded2.mutations == shard2_mutations
      assert decoded2.commit_version == @commit_version
    end

    test "handles transaction with clear_range mutations" do
      mutations = [
        {:set, "key1", "value1"},
        {:clear_range, "start", "end"},
        {:clear, "key2"}
      ]

      txn =
        Transaction.encode(%{
          mutations: mutations,
          shard_index: [{5, 3}]
        })

      assert {:ok, slices} = MutationSlicer.slice(txn, @commit_version)
      assert [{5, slice}] = slices

      {:ok, decoded} = Transaction.decode(slice)
      assert decoded.mutations == mutations
    end

    test "handles transaction with atomic mutations" do
      mutations = [
        {:atomic, :add, "counter", <<0, 0, 0, 0, 0, 0, 0, 1>>},
        {:atomic, :max, "max_val", <<0, 0, 0, 0, 0, 0, 0, 5>>}
      ]

      txn =
        Transaction.encode(%{
          mutations: mutations,
          shard_index: [{10, 2}]
        })

      assert {:ok, slices} = MutationSlicer.slice(txn, @commit_version)
      assert [{10, slice}] = slices

      {:ok, decoded} = Transaction.decode(slice)
      assert decoded.mutations == mutations
    end

    test "returns empty list for transaction without shard_index (heartbeat)" do
      txn =
        Transaction.encode(%{
          mutations: [{:set, "key", "value"}]
        })

      # Transactions without shard_index are treated as heartbeats - return empty slices
      assert {:ok, []} = MutationSlicer.slice(txn, @commit_version)
    end

    test "returns error for invalid commit version" do
      assert_raise FunctionClauseError, fn ->
        MutationSlicer.slice(<<>>, "invalid")
      end
    end
  end

  describe "slice!/2" do
    test "returns slices on success" do
      txn =
        Transaction.encode(%{
          mutations: [{:set, "key", "value"}],
          shard_index: [{0, 1}]
        })

      slices = MutationSlicer.slice!(txn, @commit_version)
      assert [{0, _slice}] = slices
    end

    test "returns empty list for transaction without shard_index (heartbeat)" do
      txn =
        Transaction.encode(%{
          mutations: [{:set, "key", "value"}]
        })

      # Transactions without shard_index are treated as heartbeats - return empty slices
      assert [] = MutationSlicer.slice!(txn, @commit_version)
    end
  end

  describe "touched_shards/1" do
    test "returns list of shard IDs" do
      txn =
        Transaction.encode(%{
          mutations: [{:set, "key1", "value1"}, {:set, "key2", "value2"}, {:set, "key3", "value3"}],
          shard_index: [{0, 1}, {5, 1}, {10, 1}]
        })

      assert {:ok, shards} = MutationSlicer.touched_shards(txn)
      assert shards == [0, 5, 10]
    end

    test "returns empty list for transaction without shard_index (heartbeat)" do
      txn =
        Transaction.encode(%{
          mutations: [{:set, "key", "value"}]
        })

      # Transactions without shard_index are treated as heartbeats
      assert {:ok, []} = MutationSlicer.touched_shards(txn)
    end
  end

  describe "edge cases" do
    test "handles empty mutations for a shard (zero count)" do
      # Edge case: shard_index with count=0 (should be rare but handle it)
      mutations = [{:set, "key", "value"}]

      txn =
        Transaction.encode(%{
          mutations: mutations,
          shard_index: [{0, 0}, {1, 1}]
        })

      assert {:ok, slices} = MutationSlicer.slice(txn, @commit_version)
      assert length(slices) == 2

      slices_map = Map.new(slices)

      # Shard 0 should have empty mutations
      {:ok, decoded0} = Transaction.decode(slices_map[0])
      assert decoded0.mutations == []

      # Shard 1 should have the mutation
      {:ok, decoded1} = Transaction.decode(slices_map[1])
      assert decoded1.mutations == mutations
    end

    test "preserves mutation order within shard" do
      mutations = [
        {:set, "key1", "value1"},
        {:set, "key2", "value2"},
        {:set, "key3", "value3"}
      ]

      txn =
        Transaction.encode(%{
          mutations: mutations,
          shard_index: [{0, 3}]
        })

      {:ok, [{0, slice}]} = MutationSlicer.slice(txn, @commit_version)
      {:ok, decoded} = Transaction.decode(slice)

      # Order should be preserved
      assert decoded.mutations == mutations
    end

    test "handles non-contiguous shard IDs" do
      mutations = [
        {:set, "key1", "value1"},
        {:set, "key2", "value2"}
      ]

      txn =
        Transaction.encode(%{
          mutations: mutations,
          shard_index: [{100, 1}, {500, 1}]
        })

      {:ok, slices} = MutationSlicer.slice(txn, @commit_version)
      shard_ids = Enum.map(slices, fn {id, _} -> id end)

      assert shard_ids == [100, 500]
    end
  end
end
