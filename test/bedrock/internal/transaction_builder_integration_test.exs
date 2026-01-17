defmodule Bedrock.Internal.TransactionBuilderIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.TransactionBuilder
  alias Bedrock.Internal.TransactionBuilder.Tx

  defmodule TestKeyCodec do
    @moduledoc false
    def encode_key(key) when is_binary(key), do: {:ok, key}
    def encode_key(_), do: :key_error
  end

  defmodule TestValueCodec do
    @moduledoc false
    def encode_value(value), do: {:ok, value}
    def decode_value(value), do: {:ok, value}
  end

  def create_test_transaction_system_layout do
    %{
      epoch: 1,
      sequencer: :test_sequencer,
      proxies: [:test_proxy1, :test_proxy2],
      storage_teams: [
        %{
          key_range: {"", <<0xFF, 0xFF>>},
          storage_ids: ["storage1", "storage2"]
        }
      ],
      services: %{
        "storage1" => %{kind: :storage, status: {:up, :test_storage1_pid}},
        "storage2" => %{kind: :storage, status: {:up, :test_storage2_pid}}
      }
    }
  end

  def start_transaction_builder(opts \\ []) do
    default_opts = [
      transaction_system_layout: create_test_transaction_system_layout()
    ]

    opts = Keyword.merge(default_opts, opts)
    start_supervised!({TransactionBuilder, opts})
  end

  defp extract_commit_result(state) do
    state.tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
  end

  defp perform_operation_and_wait(pid, operation) do
    GenServer.cast(pid, operation)
    # Sync by draining message queue
    _ = :sys.get_state(pid)
  end

  defp assert_key_value(pid, key, expected_value) do
    assert {:ok, ^expected_value} = GenServer.call(pid, {:get, key})
  end

  defp setup_nested_transaction(pid) do
    assert :ok = GenServer.call(pid, :nested_transaction)
  end

  describe "end-to-end transaction flows" do
    test "simple put -> fetch -> commit flow" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "test_key", "test_value"})
      # assert_key_value uses GenServer.call which synchronizes
      assert_key_value(pid, "test_key", "test_value")

      state = :sys.get_state(pid)

      assert %{
               mutations: [{:set, "test_key", "test_value"}],
               write_conflicts: [{"test_key", "test_key\0"}],
               read_conflicts: {nil, []}
             } = extract_commit_result(state)

      # Commit will fail due to missing infrastructure, but we can verify it tries
      assert {:error, _reason} = GenServer.call(pid, :commit)
    end

    test "multiple operations in sequence" do
      pid = start_transaction_builder()

      perform_operation_and_wait(pid, {:set_key, "key1", "value1"})
      perform_operation_and_wait(pid, {:set_key, "key2", "value2"})
      perform_operation_and_wait(pid, {:set_key, "key3", "value3"})

      assert_key_value(pid, "key1", "value1")
      assert_key_value(pid, "key2", "value2")
      assert_key_value(pid, "key3", "value3")

      state = :sys.get_state(pid)

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:set, "key2", "value2"},
                 {:set, "key3", "value3"}
               ]
             } = extract_commit_result(state)
    end

    test "put overwrite behavior" do
      pid = start_transaction_builder()

      perform_operation_and_wait(pid, {:set_key, "key", "initial_value"})
      assert_key_value(pid, "key", "initial_value")

      perform_operation_and_wait(pid, {:set_key, "key", "updated_value"})
      assert_key_value(pid, "key", "updated_value")

      state = :sys.get_state(pid)

      assert %{mutations: [{:set, "key", "updated_value"}]} = extract_commit_result(state)
    end
  end

  describe "nested transaction integration" do
    test "nested transaction with operations across levels" do
      pid = start_transaction_builder()

      perform_operation_and_wait(pid, {:set_key, "base_key", "base_value"})
      setup_nested_transaction(pid)

      GenServer.cast(pid, {:set_key, "nested_key", "nested_value"})
      GenServer.cast(pid, {:set_key, "base_key", "overwritten_value"})

      assert_key_value(pid, "base_key", "overwritten_value")
      assert_key_value(pid, "nested_key", "nested_value")

      state = :sys.get_state(pid)
      assert %{stack: [_]} = state

      assert %{
               mutations: [
                 {:set, "nested_key", "nested_value"},
                 {:set, "base_key", "overwritten_value"}
               ]
             } = extract_commit_result(state)
    end

    test "rollback restores previous transaction state" do
      pid = start_transaction_builder()

      perform_operation_and_wait(pid, {:set_key, "persistent_key", "persistent_value"})
      setup_nested_transaction(pid)

      GenServer.cast(pid, {:set_key, "temporary_key", "temporary_value"})
      GenServer.cast(pid, {:set_key, "persistent_key", "modified_value"})

      assert_key_value(pid, "persistent_key", "modified_value")
      assert_key_value(pid, "temporary_key", "temporary_value")

      GenServer.cast(pid, :rollback)

      state = :sys.get_state(pid)
      assert %{stack: []} = state
      assert_key_value(pid, "persistent_key", "persistent_value")
      assert Process.alive?(pid)
    end

    test "multiple nested levels with rollback" do
      pid = start_transaction_builder()

      perform_operation_and_wait(pid, {:set_key, "level0", "value0"})

      setup_nested_transaction(pid)
      perform_operation_and_wait(pid, {:set_key, "level1", "value1"})

      setup_nested_transaction(pid)
      perform_operation_and_wait(pid, {:set_key, "level2", "value2"})

      assert_key_value(pid, "level0", "value0")
      assert_key_value(pid, "level1", "value1")
      assert_key_value(pid, "level2", "value2")

      GenServer.cast(pid, :rollback)

      state = :sys.get_state(pid)
      assert %{stack: [_]} = state

      # Level 2 operations should be gone, but level 0 and 1 should remain
      assert_key_value(pid, "level0", "value0")
      assert_key_value(pid, "level1", "value1")
    end
  end

  describe "error propagation through system" do
    @tag :capture_log
    test "invalid key type causes process crash" do
      # This test validates fail-fast behavior - invalid keys should crash TransactionBuilder
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Ensure process is fully initialized before sending problematic message
      :sys.get_state(pid)

      # Put with invalid key type should cause process to crash
      GenServer.cast(pid, {:set_key, :invalid_key, "value"})

      # Process should crash with function_clause due to guard validation
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 1000
      assert match?({:function_clause, _}, reason)
    end

    @tag :capture_log
    test "fetch operations with unavailable sequencer return failure" do
      # This test validates error handling - unavailable sequencer should return failure, not crash
      pid = start_transaction_builder()

      # Fetch should return failure when sequencer is unavailable (it's just a stub atom)
      assert {:failure, :unavailable} = GenServer.call(pid, {:get, "non_existent_key"})

      # Process should still be alive and functional
      assert Process.alive?(pid)
    end

    test "commit with no writes succeeds with version 0" do
      pid = start_transaction_builder()

      assert {:ok, 0} = GenServer.call(pid, :commit)

      refute Process.alive?(pid)
    end
  end

  describe "process lifecycle integration" do
    test "process state consistency during complex operations" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "key1", "value1"})
      setup_nested_transaction(pid)
      GenServer.cast(pid, {:set_key, "key2", "value2"})
      setup_nested_transaction(pid)
      GenServer.cast(pid, {:set_key, "key3", "value3"})

      state = :sys.get_state(pid)
      assert %{state: :valid, stack: [_, _]} = state
      assert Process.alive?(pid)

      assert_key_value(pid, "key1", "value1")
      assert_key_value(pid, "key2", "value2")
      assert_key_value(pid, "key3", "value3")
    end

    test "timeout handling maintains state consistency" do
      pid = start_transaction_builder()

      perform_operation_and_wait(pid, {:set_key, "test_key", "test_value"})

      initial_state = :sys.get_state(pid)
      assert %{state: :valid} = initial_state

      ref = Process.monitor(pid)
      send(pid, :timeout)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "rollback on empty stack terminates process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      perform_operation_and_wait(pid, {:set_key, "key", "value"})
      GenServer.cast(pid, :rollback)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end
  end

  describe "configuration preservation" do
    test "transaction system layout preserved across operations" do
      custom_layout = %{
        sequencer: :custom_sequencer,
        proxies: [:custom_proxy],
        storage_teams: [],
        services: %{}
      }

      pid = start_transaction_builder(transaction_system_layout: custom_layout)

      GenServer.cast(pid, {:set_key, "key", "value"})
      setup_nested_transaction(pid)
      GenServer.cast(pid, {:set_key, "key2", "value2"})

      state = :sys.get_state(pid)
      assert %{transaction_system_layout: ^custom_layout} = state
    end
  end
end
