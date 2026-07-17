defmodule Bedrock.Internal.TransactionBuilder.PointReadsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.PointReads
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.Key
  alias Bedrock.KeySelector

  defp build_state do
    %State{
      layout_index: %LayoutIndex{
        tree: :gb_trees.from_orddict([{<<0xFF, 0xFF>>, {"", [self()]}}])
      },
      read_version: Version.from_integer(42)
    }
  end

  test "point read that misses storage still records a read conflict" do
    key = "missing-key"
    state = build_state()

    storage_get_key_fn = fn _pid, ^key, _version, _opts ->
      {:error, :not_found}
    end

    assert {%State{tx: tx}, {:error, :not_found}} =
             PointReads.get_key(state, key, storage_get_key_fn: storage_get_key_fn)

    assert {key, Key.key_after(key)} in tx.range_reads
  end

  test "point read that hits storage records a read conflict" do
    key = "present-key"
    state = build_state()

    storage_get_key_fn = fn _pid, ^key, _version, _opts ->
      {:ok, "value"}
    end

    assert {%State{tx: tx}, {:ok, {^key, "value"}}} =
             PointReads.get_key(state, key, storage_get_key_fn: storage_get_key_fn)

    assert {key, Key.key_after(key)} in tx.range_reads
  end

  test "snapshot point read that misses storage records no read conflict" do
    key = "missing-key"
    state = build_state()

    storage_get_key_fn = fn _pid, ^key, _version, _opts ->
      {:error, :not_found}
    end

    assert {%State{tx: tx}, {:error, :not_found}} =
             PointReads.get_key(state, key,
               storage_get_key_fn: storage_get_key_fn,
               snapshot: true
             )

    assert tx.range_reads == []
    assert tx.reads == %{}
  end

  test "snapshot point read that hits storage records no read conflict" do
    key = "present-key"
    state = build_state()

    storage_get_key_fn = fn _pid, ^key, _version, _opts ->
      {:ok, "value"}
    end

    assert {%State{tx: tx}, {:ok, {^key, "value"}}} =
             PointReads.get_key(state, key,
               storage_get_key_fn: storage_get_key_fn,
               snapshot: true
             )

    assert tx.range_reads == []
    assert tx.reads == %{}
  end

  describe "key-selector read conflicts" do
    test "forward selector hit records the span from anchor to resolved key" do
      selector = KeySelector.first_greater_or_equal("m")
      state = build_state()

      storage_fn = fn _pid, ^selector, _version, _opts ->
        {:ok, {"moose", "antlers"}}
      end

      assert {%State{tx: tx}, {:ok, {"moose", "antlers"}}} =
               PointReads.get_key_selector(state, selector, storage_get_key_selector_fn: storage_fn)

      assert {"m", Key.key_after("moose")} in tx.range_reads
      assert %{"moose" => "antlers"} = tx.reads
    end

    test "backward selector hit records the span from resolved key to anchor" do
      selector = KeySelector.last_less_or_equal("m")
      state = build_state()

      storage_fn = fn _pid, ^selector, _version, _opts ->
        {:ok, {"kangaroo", "pouch"}}
      end

      assert {%State{tx: tx}, {:ok, {"kangaroo", "pouch"}}} =
               PointReads.get_key_selector(state, selector, storage_get_key_selector_fn: storage_fn)

      assert {"kangaroo", Key.key_after("m")} in tx.range_reads
    end

    test "selector that resolves to nothing records the scanned shard range" do
      selector = KeySelector.first_greater_or_equal("zzz")
      state = build_state()

      storage_fn = fn _pid, ^selector, _version, _opts ->
        {:ok, nil}
      end

      assert {%State{tx: tx}, {:error, :not_found}} =
               PointReads.get_key_selector(state, selector, storage_get_key_selector_fn: storage_fn)

      assert {"", <<0xFF, 0xFF>>} in tx.range_reads
    end

    test "snapshot selector reads record no conflicts on hit or miss" do
      selector = KeySelector.first_greater_or_equal("m")
      state = build_state()

      hit_fn = fn _pid, ^selector, _version, _opts -> {:ok, {"moose", "antlers"}} end
      miss_fn = fn _pid, ^selector, _version, _opts -> {:ok, nil} end

      assert {%State{tx: hit_tx}, {:ok, _}} =
               PointReads.get_key_selector(state, selector,
                 storage_get_key_selector_fn: hit_fn,
                 snapshot: true
               )

      assert {%State{tx: miss_tx}, {:error, :not_found}} =
               PointReads.get_key_selector(state, selector,
                 storage_get_key_selector_fn: miss_fn,
                 snapshot: true
               )

      assert hit_tx.range_reads == []
      assert miss_tx.range_reads == []
      assert miss_tx.reads == %{}
    end

    test "selector miss reaches the resolver as a committed read conflict" do
      selector = KeySelector.first_greater_or_equal("zzz")
      state = build_state()

      storage_fn = fn _pid, ^selector, _version, _opts -> {:ok, nil} end

      {%State{tx: tx, read_version: read_version}, {:error, :not_found}} =
        PointReads.get_key_selector(state, selector, storage_get_key_selector_fn: storage_fn)

      encoded = Tx.commit(tx, read_version)

      assert {:ok, {^read_version, read_conflicts}} = Transaction.read_conflicts(encoded)
      assert {"", <<0xFF, 0xFF>>} in read_conflicts
    end
  end

  test "missed point read reaches the resolver as a committed read conflict" do
    key = "missing-key"
    state = build_state()

    storage_get_key_fn = fn _pid, ^key, _version, _opts ->
      {:error, :not_found}
    end

    {%State{tx: tx, read_version: read_version}, {:error, :not_found}} =
      PointReads.get_key(state, key, storage_get_key_fn: storage_get_key_fn)

    encoded = Tx.commit(tx, read_version)

    assert {:ok, {^read_version, read_conflicts}} = Transaction.read_conflicts(encoded)
    assert {key, Key.key_after(key)} in read_conflicts
  end
end
