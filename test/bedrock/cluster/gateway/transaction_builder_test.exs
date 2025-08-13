defmodule Bedrock.Cluster.Gateway.TransactionBuilderTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  defmodule TestKeyCodec do
    def encode_key(key) when is_binary(key), do: {:ok, key}
    def encode_key(_), do: :key_error
  end

  defmodule TestValueCodec do
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
      transaction_system_layout: create_test_transaction_system_layout(),
      key_codec: TestKeyCodec,
      value_codec: TestValueCodec
    ]

    opts = Keyword.merge(default_opts, opts)
    {:ok, pid} = TransactionBuilder.start_link(opts)
    pid
  end

  describe "TransactionBuilder GenServer lifecycle" do
    test "starts successfully with valid options" do
      pid = start_transaction_builder()
      assert Process.alive?(pid)

      # Verify initial state
      state = :sys.get_state(pid)
      assert %State{} = state
      assert state.state == :valid
      assert state.gateway == self()
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
      assert state.reads == %{}
      assert state.writes == %{}
      assert state.stack == []
    end

    test "fails to start with missing required options" do
      assert_raise KeyError, fn ->
        TransactionBuilder.start_link([])
      end

      assert_raise KeyError, fn ->
        TransactionBuilder.start_link(gateway: self())
      end
    end

    test "handles timeout by stopping normally" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      send(pid, :timeout)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end
  end

  describe "nested transactions" do
    test "nested_transaction creates stack frame" do
      pid = start_transaction_builder()

      # Add some initial reads and writes
      GenServer.cast(pid, {:put, "key1", "value1"})

      # Wait for the put to process
      :timer.sleep(10)

      state_before = :sys.get_state(pid)
      assert state_before.writes == %{"key1" => "value1"}

      # Start nested transaction
      :ok = GenServer.call(pid, :nested_transaction)

      state_after = :sys.get_state(pid)
      assert state_after.reads == %{}
      assert state_after.writes == %{}
      assert length(state_after.stack) == 1
      # Stack should contain the previous reads and writes
      assert state_after.stack == [{%{}, %{"key1" => "value1"}}]
    end

    test "multiple nested transactions create multiple stack frames" do
      pid = start_transaction_builder()

      # Add initial data
      GenServer.cast(pid, {:put, "key1", "value1"})
      :timer.sleep(10)

      # First nested transaction
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "key2", "value2"})
      :timer.sleep(10)

      # Second nested transaction
      :ok = GenServer.call(pid, :nested_transaction)

      state = :sys.get_state(pid)
      assert length(state.stack) == 2
      assert state.reads == %{}
      assert state.writes == %{}
    end

    test "rollback with empty stack stops the process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      GenServer.cast(pid, :rollback)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "rollback with non-empty stack pops stack frame" do
      pid = start_transaction_builder()

      # Create nested transaction
      GenServer.cast(pid, {:put, "key1", "value1"})
      :timer.sleep(10)
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "key2", "value2"})
      :timer.sleep(10)

      state_before = :sys.get_state(pid)
      assert length(state_before.stack) == 1
      assert state_before.writes == %{"key2" => "value2"}

      # Rollback should pop the stack
      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      state_after = :sys.get_state(pid)
      assert Enum.empty?(state_after.stack)
      # The process should still be alive after rollback from nested transaction
      assert Process.alive?(pid)
    end
  end

  describe "put operations" do
    test "put with valid key and value updates writes" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "test_key", "test_value"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.writes == %{"test_key" => "test_value"}
    end

    test "handles valid key types" do
      pid = start_transaction_builder()

      # Test with various valid binary keys
      GenServer.cast(pid, {:put, "simple_key", "value1"})
      # empty string
      GenServer.cast(pid, {:put, "", "value2"})
      # unicode
      GenServer.cast(pid, {:put, "unicode_key_🔑", "value3"})
      :timer.sleep(10)

      state = :sys.get_state(pid)

      assert state.writes == %{
               "simple_key" => "value1",
               "" => "value2",
               "unicode_key_🔑" => "value3"
             }
    end

    test "multiple puts accumulate in writes" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "key1", "value1"})
      GenServer.cast(pid, {:put, "key2", "value2"})
      GenServer.cast(pid, {:put, "key3", "value3"})
      :timer.sleep(10)

      state = :sys.get_state(pid)

      assert state.writes == %{
               "key1" => "value1",
               "key2" => "value2",
               "key3" => "value3"
             }
    end

    test "put overwrites previous value for same key" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "key1", "value1"})
      GenServer.cast(pid, {:put, "key1", "updated_value"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.writes == %{"key1" => "updated_value"}
    end
  end

  describe "fetch operations" do
    test "fetch returns value from writes cache" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "cached_key", "cached_value"})
      :timer.sleep(10)

      result = GenServer.call(pid, {:fetch, "cached_key"})
      assert result == {:ok, "cached_value"}
    end

    test "fetch operations maintain state consistency" do
      pid = start_transaction_builder()

      # First put a value that we can fetch from cache
      GenServer.cast(pid, {:put, "cached_key", "cached_value"})
      :timer.sleep(10)

      # Fetch should return the cached value
      result = GenServer.call(pid, {:fetch, "cached_key"})
      assert result == {:ok, "cached_value"}

      # Verify the process is still alive and state is consistent
      state = :sys.get_state(pid)
      assert state.writes == %{"cached_key" => "cached_value"}
      assert Process.alive?(pid)
    end
  end

  describe "commit operations" do
    test "commit with no changes returns error" do
      pid = start_transaction_builder()

      # Empty transaction should fail to commit
      result = GenServer.call(pid, :commit)

      # Should get an error because there are no proxies available or similar infrastructure issue
      assert {:error, _reason} = result
    end

    test "commit with writes attempts to commit" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "test_key", "test_value"})
      :timer.sleep(10)

      # This will likely fail because we haven't mocked the full commit infrastructure
      result = GenServer.call(pid, :commit)

      # The exact error depends on the implementation, but it should be an error
      assert {:error, _reason} = result
    end
  end

  describe "state management" do
    test "maintains consistent state during operations" do
      pid = start_transaction_builder()

      # Verify initial state is valid
      state = :sys.get_state(pid)
      assert state.state == :valid
      assert state.read_version == nil

      # Add some operations and verify state remains consistent
      GenServer.cast(pid, {:put, "key1", "value1"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "key2", "value2"})
      :timer.sleep(10)

      final_state = :sys.get_state(pid)
      assert final_state.state == :valid
      assert length(final_state.stack) == 1
      assert final_state.writes == %{"key2" => "value2"}
    end
  end

  describe "state preservation" do
    test "preserves transaction_system_layout across operations" do
      custom_layout = %{
        sequencer: :custom_sequencer,
        proxies: [:custom_proxy],
        storage_teams: [],
        services: %{}
      }

      pid = start_transaction_builder(transaction_system_layout: custom_layout)

      # Perform some operations
      GenServer.cast(pid, {:put, "key", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
    end

    test "preserves codec configuration across operations" do
      defmodule CustomKeyCodec do
        def encode_key(key), do: {:ok, "custom:#{key}"}
      end

      defmodule CustomValueCodec do
        def encode_value(value), do: {:ok, "encoded:#{value}"}
        def decode_value(value), do: {:ok, value}
      end

      pid =
        start_transaction_builder(
          key_codec: CustomKeyCodec,
          value_codec: CustomValueCodec
        )

      GenServer.cast(pid, {:put, "test", "value"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.key_codec == CustomKeyCodec
      assert state.value_codec == CustomValueCodec
      # Verify the custom codec was used
      assert state.writes == %{"custom:test" => "encoded:value"}
    end
  end
end
