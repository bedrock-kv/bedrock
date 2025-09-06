defmodule Bedrock.Cluster.Gateway.TransactionBuilderIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

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
      sequencer: :test_sequencer,
      proxies: [:test_proxy1, :test_proxy2],
      storage_teams: [
        %{
          key_range: {"", :end},
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
      gateway: self(),
      transaction_system_layout: create_test_transaction_system_layout()
    ]

    opts = Keyword.merge(default_opts, opts)
    start_supervised!({TransactionBuilder, opts})
  end

  describe "end-to-end transaction flows" do
    test "simple put -> fetch -> commit flow" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "test_key", "test_value"})
      :timer.sleep(10)

      result = GenServer.call(pid, {:get, "test_key"})
      assert result == {:ok, "test_value"}

      state = :sys.get_state(pid)

      assert Tx.commit(state.tx) == %{
               mutations: [{:set, "test_key", "test_value"}],
               write_conflicts: [{"test_key", "test_key\0"}],
               read_conflicts: {nil, []}
             }

      # Commit will fail due to missing infrastructure, but we can verify it tries
      result = GenServer.call(pid, :commit)
      assert {:error, _reason} = result
    end

    test "multiple operations in sequence" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "key1", "value1"})
      GenServer.cast(pid, {:set_key, "key2", "value2"})
      GenServer.cast(pid, {:set_key, "key3", "value3"})
      :timer.sleep(10)

      assert GenServer.call(pid, {:get, "key1"}) == {:ok, "value1"}
      assert GenServer.call(pid, {:get, "key2"}) == {:ok, "value2"}
      assert GenServer.call(pid, {:get, "key3"}) == {:ok, "value3"}

      state = :sys.get_state(pid)
      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "key1", "value1"},
               {:set, "key2", "value2"},
               {:set, "key3", "value3"}
             ]
    end

    test "put overwrite behavior" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "key", "initial_value"})
      :timer.sleep(10)

      assert GenServer.call(pid, {:get, "key"}) == {:ok, "initial_value"}

      GenServer.cast(pid, {:set_key, "key", "updated_value"})
      :timer.sleep(10)

      assert GenServer.call(pid, {:get, "key"}) == {:ok, "updated_value"}

      state = :sys.get_state(pid)
      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "key", "updated_value"}
             ]
    end
  end

  describe "nested transaction integration" do
    test "nested transaction with operations across levels" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "base_key", "base_value"})
      :timer.sleep(10)

      :ok = GenServer.call(pid, :nested_transaction)

      GenServer.cast(pid, {:set_key, "nested_key", "nested_value"})
      GenServer.cast(pid, {:set_key, "base_key", "overwritten_value"})
      :timer.sleep(10)

      assert GenServer.call(pid, {:get, "base_key"}) == {:ok, "overwritten_value"}
      assert GenServer.call(pid, {:get, "nested_key"}) == {:ok, "nested_value"}

      state = :sys.get_state(pid)
      assert length(state.stack) == 1

      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "nested_key", "nested_value"},
               {:set, "base_key", "overwritten_value"}
             ]
    end

    test "rollback restores previous transaction state" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "persistent_key", "persistent_value"})
      :timer.sleep(10)

      :ok = GenServer.call(pid, :nested_transaction)

      GenServer.cast(pid, {:set_key, "temporary_key", "temporary_value"})
      GenServer.cast(pid, {:set_key, "persistent_key", "modified_value"})
      :timer.sleep(10)

      assert GenServer.call(pid, {:get, "persistent_key"}) == {:ok, "modified_value"}
      assert GenServer.call(pid, {:get, "temporary_key"}) == {:ok, "temporary_value"}

      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.stack == []

      assert GenServer.call(pid, {:get, "persistent_key"}) == {:ok, "persistent_value"}

      assert Process.alive?(pid)
    end

    test "multiple nested levels with rollback" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "level0", "value0"})
      :timer.sleep(10)

      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:set_key, "level1", "value1"})
      :timer.sleep(10)

      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:set_key, "level2", "value2"})
      :timer.sleep(10)

      assert GenServer.call(pid, {:get, "level0"}) == {:ok, "value0"}
      assert GenServer.call(pid, {:get, "level1"}) == {:ok, "value1"}
      assert GenServer.call(pid, {:get, "level2"}) == {:ok, "value2"}

      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert length(state.stack) == 1

      # Level 2 operations should be gone, but level 0 and 1 should remain
      assert GenServer.call(pid, {:get, "level0"}) == {:ok, "value0"}
      assert GenServer.call(pid, {:get, "level1"}) == {:ok, "value1"}
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
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}
      assert match?({:function_clause, _}, reason)
    end

    @tag :capture_log
    test "fetch operations with missing keys cause runtime error" do
      # This test validates fail-fast behavior - missing read version should crash TransactionBuilder
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Fetch should cause exit due to RuntimeError in GenServer
      catch_exit(GenServer.call(pid, {:get, "non_existent_key"}))

      # Process should crash with RuntimeError about no read version
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}

      assert match?(
               {%RuntimeError{message: "No read version available"}, _},
               reason
             )
    end

    test "commit with no writes returns error" do
      pid = start_transaction_builder()

      result = GenServer.call(pid, :commit)

      assert {:error, _reason} = result

      assert Process.alive?(pid)
    end
  end

  describe "process lifecycle integration" do
    test "process state consistency during complex operations" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "key1", "value1"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:set_key, "key2", "value2"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:set_key, "key3", "value3"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.state == :valid
      assert length(state.stack) == 2
      assert Process.alive?(pid)

      assert GenServer.call(pid, {:get, "key1"}) == {:ok, "value1"}
      assert GenServer.call(pid, {:get, "key2"}) == {:ok, "value2"}
      assert GenServer.call(pid, {:get, "key3"}) == {:ok, "value3"}
    end

    test "timeout handling maintains state consistency" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:set_key, "test_key", "test_value"})
      :timer.sleep(10)

      initial_state = :sys.get_state(pid)
      assert initial_state.state == :valid

      ref = Process.monitor(pid)
      send(pid, :timeout)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "rollback on empty stack terminates process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      GenServer.cast(pid, {:set_key, "key", "value"})
      :timer.sleep(10)

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
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:set_key, "key2", "value2"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
    end
  end
end
