defmodule Bedrock.Cluster.Gateway.TransactionBuilder.PuttingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Putting
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  # Test codecs removed since we no longer use them

  defp mutations_to_writes(mutations) do
    Enum.reduce(mutations, %{}, fn
      {:set, key, value}, acc -> Map.put(acc, key, value)
      _, acc -> acc
    end)
  end

  def create_test_state(_writes \\ %{}) do
    %State{
      state: :valid,
      gateway: self(),
      transaction_system_layout: %{}
    }
  end

  describe "do_put/3" do
    test "successfully puts a binary key and value" do
      state = create_test_state()

      {:ok, new_state} = Putting.set_key(state, "test_key", "test_value")

      assert mutations_to_writes(Tx.commit(new_state.tx).mutations) == %{
               "test_key" => "test_value"
             }
    end

    test "successfully puts multiple key-value pairs" do
      state = create_test_state()

      {:ok, state1} = Putting.set_key(state, "key1", "value1")
      {:ok, state2} = Putting.set_key(state1, "key2", "value2")
      {:ok, state3} = Putting.set_key(state2, "key3", "value3")

      assert mutations_to_writes(Tx.commit(state3.tx).mutations) == %{
               "key1" => "value1",
               "key2" => "value2",
               "key3" => "value3"
             }
    end

    test "overwrites existing key with new value" do
      state = create_test_state(%{"existing_key" => "old_value"})

      {:ok, new_state} = Putting.set_key(state, "existing_key", "new_value")

      assert mutations_to_writes(Tx.commit(new_state.tx).mutations) == %{
               "existing_key" => "new_value"
             }
    end

    test "handles binary values" do
      state = create_test_state()

      {:ok, new_state} = Putting.set_key(state, "number_key", "42")

      assert mutations_to_writes(Tx.commit(new_state.tx).mutations) == %{
               "number_key" => "42"
             }
    end

    test "handles binary values directly" do
      state = create_test_state()

      {:ok, state1} = Putting.set_key(state, "string", "text")
      {:ok, state2} = Putting.set_key(state1, "binary", "data")

      assert mutations_to_writes(Tx.commit(state2.tx).mutations) == %{
               "string" => "text",
               "binary" => "data"
             }
    end

    test "returns :key_error for non-binary key" do
      state = create_test_state()

      result = Putting.set_key(state, :invalid_key, "value")

      assert result == :key_error
    end

    test "handles empty string key and value" do
      state = create_test_state()

      {:ok, new_state} = Putting.set_key(state, "", "")

      assert mutations_to_writes(Tx.commit(new_state.tx).mutations) == %{"" => ""}
    end

    test "handles unicode keys and values" do
      state = create_test_state()

      {:ok, new_state} = Putting.set_key(state, "键名", "值")

      assert mutations_to_writes(Tx.commit(new_state.tx).mutations) == %{"键名" => "值"}
    end

    test "preserves other state fields" do
      # Create transaction with existing data
      existing_tx = Tx.set(Tx.new(), "existing", "value")

      original_state = %State{
        state: :valid,
        gateway: self(),
        transaction_system_layout: %{test: "layout"},
        tx: existing_tx,
        stack: [],
        fastest_storage_servers: %{range: :server}
      }

      {:ok, new_state} = Putting.set_key(original_state, "new_key", "new_value")

      # Verify writes were updated - should have both existing and new values
      result_writes = mutations_to_writes(Tx.commit(new_state.tx).mutations)
      assert result_writes == %{"existing" => "value", "new_key" => "new_value"}

      # Verify other fields preserved
      assert new_state.state == :valid
      assert new_state.gateway == original_state.gateway
      assert new_state.transaction_system_layout == original_state.transaction_system_layout
      assert new_state.stack == original_state.stack
      assert new_state.fastest_storage_servers == original_state.fastest_storage_servers
    end

    test "handles large keys and values" do
      state = create_test_state()
      large_key = String.duplicate("k", 1000)
      large_value = String.duplicate("v", 10_000)

      {:ok, new_state} = Putting.set_key(state, large_key, large_value)

      assert mutations_to_writes(Tx.commit(new_state.tx).mutations) == %{
               large_key => large_value
             }
    end

    test "works with binary data containing null bytes" do
      state = create_test_state()
      binary_key = "\x00\x01\xFF\x02"
      binary_value = "\xFF\x00\x01\x02"

      {:ok, new_state} = Putting.set_key(state, binary_key, binary_value)

      assert mutations_to_writes(Tx.commit(new_state.tx).mutations) == %{
               binary_key => binary_value
             }
    end
  end

  describe "error handling" do
    test "validates key is binary" do
      state = create_test_state()

      result = Putting.set_key(state, :invalid_key, "value")

      assert result == :key_error
    end

    test "accepts any value when key is binary" do
      state = create_test_state()

      {:ok, new_state} = Putting.set_key(state, "key", "value")

      assert mutations_to_writes(Tx.commit(new_state.tx).mutations) == %{"key" => "value"}
    end
  end
end
