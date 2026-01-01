defmodule Bedrock.Internal.TransactionBuilder.TxBinaryIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.TransactionBuilder.Tx

  describe "binary transaction integration" do
    test "commit returns binary transaction" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")
        |> Tx.clear("key3")

      binary_result = Tx.commit(tx, nil)
      assert is_binary(binary_result)
      assert {:ok, _validated} = Transaction.validate(binary_result)

      # Use pattern matching to verify complete structure in one assertion
      assert {:ok,
              %{
                mutations: [
                  {:set, "key1", "value1"},
                  {:set, "key2", "value2"},
                  {:clear, "key3"}
                ],
                write_conflicts: [
                  {"key1", "key1\0"},
                  {"key2", "key2\0"},
                  {"key3", "key3\0"}
                ],
                read_conflicts: {nil, []}
              }} = Transaction.decode(binary_result)
    end

    test "transaction with reads generates read conflicts" do
      # Mock fetch function that returns values
      fetch_fn = fn
        "existing_key", state -> {{:ok, "existing_value"}, state}
        _, state -> {{:error, :not_found}, state}
      end

      tx = Tx.new()

      # Add some reads and write using pattern matching for cleaner assertions
      assert {tx, {:ok, _value}, _state} = Tx.get(tx, "existing_key", fetch_fn, :state)
      assert {tx, {:error, :not_found}, _state} = Tx.get(tx, "missing_key", fetch_fn, :state)
      tx = Tx.set(tx, "new_key", "new_value")

      read_version = Bedrock.DataPlane.Version.from_integer(12_345)
      binary_result = Tx.commit(tx, read_version)

      # Use pattern matching to verify complete structure
      assert {:ok,
              %{
                mutations: [{:set, "new_key", "new_value"}],
                read_conflicts:
                  {^read_version,
                   [
                     {"existing_key", "existing_key\0"},
                     {"missing_key", "missing_key\0"}
                   ]},
                write_conflicts: [{"new_key", "new_key\0"}]
              }} = Transaction.decode(binary_result)
    end

    test "transaction with range operations" do
      tx =
        Tx.new()
        |> Tx.clear_range("end_key", "start_key")
        |> Tx.set("inside_range", "value")

      binary_result = Tx.commit(tx, nil)

      # Use pattern matching to verify complete structure
      assert {:ok,
              %{
                mutations: [
                  {:clear_range, "end_key", "start_key"},
                  {:set, "inside_range", "value"}
                ],
                write_conflicts: [{"end_key", "start_key"}]
              }} = Transaction.decode(binary_result)
    end

    test "empty transaction produces valid binary" do
      binary_result = Tx.commit(Tx.new(), nil)

      assert is_binary(binary_result)
      # Use pattern matching to verify empty structure
      assert {:ok,
              %{
                mutations: [],
                read_conflicts: {nil, []},
                write_conflicts: []
              }} = Transaction.decode(binary_result)
    end

    # Range read testing moved to client-side streaming architecture

    test "binary transaction maintains size optimization" do
      # Create transactions with different key/value sizes and commit them
      binaries = [
        {"small", Tx.commit(Tx.set(Tx.new(), "k", "v"), nil)},
        {"medium", Tx.commit(Tx.set(Tx.new(), "k", String.duplicate("x", 300)), nil)},
        {"large", Tx.commit(Tx.set(Tx.new(), String.duplicate("k", 300), String.duplicate("v", 70_000)), nil)}
      ]

      # All should decode correctly
      for {_size, binary} <- binaries do
        assert {:ok, _decoded} = Transaction.decode(binary)
      end

      # Size optimization: smaller data should result in smaller binaries
      assert [small_binary, medium_binary, large_binary] = Enum.map(binaries, &elem(&1, 1))
      assert byte_size(small_binary) < byte_size(medium_binary)
      assert byte_size(medium_binary) < byte_size(large_binary)
    end

    test "transaction builder integrates with Transaction section operations" do
      binary_result =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")
        |> Tx.commit(nil)

      # Verify section operations work using pattern matching
      assert {:ok, mutations_section} = Transaction.extract_section(binary_result, 0x01)
      assert is_binary(mutations_section) and byte_size(mutations_section) > 0

      assert {:ok, stream} = Transaction.mutations(binary_result)
      assert [_mutation1, _mutation2] = Enum.to_list(stream)

      # Verify version stamping preserves original data
      version = Bedrock.DataPlane.Version.from_integer(12_345)
      assert {:ok, stamped} = Transaction.add_commit_version(binary_result, version)
      assert {:ok, ^version} = Transaction.commit_version(stamped)

      # Original transaction data should be preserved after stamping
      assert {:ok,
              %{
                mutations: [
                  {:set, "key1", "value1"},
                  {:set, "key2", "value2"}
                ]
              }} = Transaction.decode(stamped)
    end
  end
end
