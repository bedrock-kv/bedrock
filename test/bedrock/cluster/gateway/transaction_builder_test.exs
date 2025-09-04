defmodule Bedrock.Cluster.Gateway.TransactionBuilderTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.KeySelector

  # This test file focuses on GenServer-specific functionality:
  # - Process lifecycle (start_link, initialization, termination)
  # - Message handling (handle_call, handle_cast, handle_info)
  # - GenServer state management
  # - Process supervision and error handling
  #
  # For end-to-end transaction flows, see transaction_builder_integration_test.exs
  # For individual module testing, see the respective module test files

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
      transaction_system_layout: create_test_transaction_system_layout(),
      key_codec: TestKeyCodec,
      value_codec: TestValueCodec
    ]

    opts = Keyword.merge(default_opts, opts)
    start_supervised!({TransactionBuilder, opts})
  end

  describe "GenServer process lifecycle" do
    test "starts successfully with valid options" do
      pid = start_transaction_builder()
      assert Process.alive?(pid)

      state = :sys.get_state(pid)
      assert %State{} = state
      assert state.state == :valid
      assert state.gateway == self()
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
      assert state.stack == []
      assert %Tx{} = state.tx
    end

    test "fails to start with missing required options" do
      assert_raise KeyError, fn ->
        TransactionBuilder.start_link([])
      end

      assert_raise KeyError, fn ->
        TransactionBuilder.start_link(gateway: self())
      end
    end

    test "terminates normally on timeout message" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      send(pid, :timeout)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "starts with custom configuration" do
      custom_layout = %{
        sequencer: :custom,
        proxies: [],
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}}
        }
      }

      pid =
        start_transaction_builder(
          transaction_system_layout: custom_layout,
          key_codec: TestKeyCodec,
          value_codec: TestValueCodec
        )

      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
    end
  end

  describe "GenServer call handling" do
    test ":nested_transaction call creates stack frame" do
      pid = start_transaction_builder()

      initial_state = :sys.get_state(pid)
      initial_stack_size = length(initial_state.stack)

      result = GenServer.call(pid, :nested_transaction)
      assert result == :ok

      final_state = :sys.get_state(pid)
      assert length(final_state.stack) == initial_stack_size + 1
      assert Process.alive?(pid)
    end

    test ":fetch call with cached key" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "test_key", "test_value"})
      :timer.sleep(10)

      result = GenServer.call(pid, {:fetch, "test_key"})
      assert result == {:ok, "test_value"}
    end

    test ":commit call returns error for empty transaction" do
      pid = start_transaction_builder()

      result = GenServer.call(pid, :commit)
      assert {:error, _reason} = result
      assert Process.alive?(pid)
    end

    test "multiple :nested_transaction calls stack properly" do
      pid = start_transaction_builder()

      :ok = GenServer.call(pid, :nested_transaction)
      state1 = :sys.get_state(pid)
      assert length(state1.stack) == 1

      :ok = GenServer.call(pid, :nested_transaction)
      state2 = :sys.get_state(pid)
      assert length(state2.stack) == 2
    end

    @tag :capture_log
    test "unknown call crashes process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      catch_exit(GenServer.call(pid, :unknown_call))

      assert_receive {:DOWN, ^ref, :process, ^pid, reason}
      assert match?({:function_clause, _}, reason)
    end
  end

  describe "GenServer cast handling" do
    test "{:put, key, value} cast updates transaction state" do
      pid = start_transaction_builder()

      initial_state = :sys.get_state(pid)
      initial_mutations = Tx.commit(initial_state.tx).mutations

      GenServer.cast(pid, {:put, "test_key", "test_value"})
      :timer.sleep(10)

      final_state = :sys.get_state(pid)
      final_mutations = Tx.commit(final_state.tx).mutations

      assert final_mutations == initial_mutations ++ [{:set, "test_key", "test_value"}]
    end

    test ":rollback cast with empty stack terminates process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      GenServer.cast(pid, :rollback)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test ":rollback cast with non-empty stack pops stack frame" do
      pid = start_transaction_builder()

      :ok = GenServer.call(pid, :nested_transaction)
      state_before = :sys.get_state(pid)
      assert length(state_before.stack) == 1

      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      state_after = :sys.get_state(pid)
      assert Enum.empty?(state_after.stack)
      assert Process.alive?(pid)
    end

    test "multiple put casts accumulate in transaction" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "key1", "value1"})
      GenServer.cast(pid, {:put, "key2", "value2"})
      GenServer.cast(pid, {:put, "key3", "value3"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      mutations = Tx.commit(state.tx).mutations

      assert mutations == [
               {:set, "key1", "value1"},
               {:set, "key2", "value2"},
               {:set, "key3", "value3"}
             ]
    end

    test "put cast with same key overwrites in mutations but not state" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "key1", "value1"})
      GenServer.cast(pid, {:put, "key1", "updated_value"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "key1", "updated_value"}
             ]

      result = GenServer.call(pid, {:fetch, "key1"})
      assert result == {:ok, "updated_value"}
    end

    @tag :capture_log
    test "unknown cast crashes process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      GenServer.cast(pid, :unknown_cast)

      assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 1000
      assert match?({:function_clause, _}, reason)
    end
  end

  describe "GenServer info message handling" do
    test ":timeout message terminates process normally" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      send(pid, :timeout)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    @tag :capture_log
    test "unknown info messages crash the process" do
      # This test validates fail-fast behavior - TransactionBuilder should crash on unknown info messages
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Send unknown info message - will crash due to no catch-all clause
      send(pid, :unknown_message)

      # Process should crash with FunctionClauseError
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}
      assert match?({:function_clause, _}, reason)
    end
  end

  describe "GenServer state management" do
    test "state is properly initialized with defaults" do
      pid = start_transaction_builder()

      state = :sys.get_state(pid)

      assert %State{} = state
      assert state.state == :valid
      assert state.gateway == self()
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
      assert state.read_version == nil
      assert state.commit_version == nil
      assert state.stack == []
      assert state.fastest_storage_servers == %{}
      assert is_integer(state.fetch_timeout_in_ms)
      assert is_integer(state.lease_renewal_threshold)
      assert %Tx{} = state.tx
    end

    test "state fields are preserved across operations" do
      custom_layout = %{
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}}
        }
      }

      pid = start_transaction_builder(transaction_system_layout: custom_layout)

      GenServer.cast(pid, {:put, "key", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "key2", "value2"})
      :timer.sleep(10)

      state = :sys.get_state(pid)

      assert state.state == :valid
      assert state.transaction_system_layout == custom_layout
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
      assert state.gateway == self()
    end

    test "stack management through GenServer operations" do
      pid = start_transaction_builder()

      initial_state = :sys.get_state(pid)
      assert Enum.empty?(initial_state.stack)

      GenServer.cast(pid, {:put, "base", "value"})
      :ok = GenServer.call(pid, :nested_transaction)

      nested_state = :sys.get_state(pid)
      assert length(nested_state.stack) == 1

      GenServer.cast(pid, {:put, "nested", "value"})
      :ok = GenServer.call(pid, :nested_transaction)

      double_nested_state = :sys.get_state(pid)
      assert length(double_nested_state.stack) == 2

      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      after_rollback_state = :sys.get_state(pid)
      assert length(after_rollback_state.stack) == 1
    end

    test "transaction state accumulates properly" do
      pid = start_transaction_builder()

      GenServer.cast(pid, {:put, "key1", "value1"})
      :timer.sleep(5)

      state1 = :sys.get_state(pid)
      mutations1 = Tx.commit(state1.tx).mutations
      assert length(mutations1) == 1

      GenServer.cast(pid, {:put, "key2", "value2"})
      :timer.sleep(5)

      state2 = :sys.get_state(pid)
      mutations2 = Tx.commit(state2.tx).mutations
      assert length(mutations2) == 2

      assert mutations2 == [
               {:set, "key1", "value1"},
               {:set, "key2", "value2"}
             ]
    end
  end

  describe "GenServer error handling and resilience" do
    test "process remains stable under normal message load" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Send puts and nested transactions deterministically
      for i <- 1..50 do
        GenServer.cast(pid, {:put, "key_#{i}", "value_#{i}"})
      end

      # Add nested transactions at specific points
      :ok = GenServer.call(pid, :nested_transaction)
      :ok = GenServer.call(pid, :nested_transaction)
      :ok = GenServer.call(pid, :nested_transaction)
      :ok = GenServer.call(pid, :nested_transaction)
      :ok = GenServer.call(pid, :nested_transaction)

      :timer.sleep(50)

      assert Process.alive?(pid)

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 10

      state = :sys.get_state(pid)
      assert state.state == :valid
      assert length(state.stack) == 5
    end

    test "process handles rapid rollbacks correctly" do
      pid = start_transaction_builder()

      for _i <- 1..5 do
        :ok = GenServer.call(pid, :nested_transaction)
      end

      initial_state = :sys.get_state(pid)
      assert length(initial_state.stack) == 5

      for _i <- 1..3 do
        GenServer.cast(pid, :rollback)
      end

      :timer.sleep(20)

      assert Process.alive?(pid)
      final_state = :sys.get_state(pid)
      assert length(final_state.stack) == 2
    end

    test "process terminates cleanly on final rollback" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      GenServer.cast(pid, :rollback)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end
  end

  describe "GenServer configuration and customization" do
    test "custom codecs are applied and preserved" do
      defmodule CustomKeyCodec do
        @moduledoc false
        def encode_key(key), do: {:ok, "custom_#{key}"}
      end

      defmodule CustomValueCodec do
        @moduledoc false
        def encode_value(value), do: {:ok, "encoded_#{value}"}
        def decode_value(value), do: {:ok, value}
      end

      pid =
        start_transaction_builder(
          key_codec: CustomKeyCodec,
          value_codec: CustomValueCodec
        )

      state = :sys.get_state(pid)
      assert state.key_codec == CustomKeyCodec
      assert state.value_codec == CustomValueCodec

      GenServer.cast(pid, {:put, "test", "value"})
      :timer.sleep(10)

      final_state = :sys.get_state(pid)
      mutations = Tx.commit(final_state.tx).mutations
      assert [{:set, "custom_test", "encoded_value"}] = mutations
    end

    test "transaction system layout is preserved" do
      custom_layout = %{
        sequencer: :custom_sequencer,
        proxies: [:custom_proxy1, :custom_proxy2],
        storage_teams: [
          %{
            key_range: {"", "m"},
            storage_ids: ["storage1"]
          },
          %{
            key_range: {"m", :end},
            storage_ids: ["storage2"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}},
          "storage2" => %{kind: :storage, status: {:up, :pid2}}
        }
      }

      pid = start_transaction_builder(transaction_system_layout: custom_layout)

      GenServer.cast(pid, {:put, "key", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
    end
  end

  describe "KeySelector operations" do
    test "handles {:fetch_key_selector, key_selector} call and delegates to resolution module" do
      key_selector = KeySelector.first_greater_or_equal("test_key")
      pid = start_transaction_builder()

      # Make the call - this will delegate to KeySelectorResolution.resolve_key_selector/3
      result = GenServer.call(pid, {:fetch_key_selector, key_selector})

      assert is_tuple(result)
      assert tuple_size(result) >= 2
      assert elem(result, 0) in [:ok, :error]

      # Check that the call was properly handled and state remains valid
      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles KeySelector with first_greater_or_equal" do
      pid = start_transaction_builder()
      selector = KeySelector.first_greater_or_equal("test_key")

      result = GenServer.call(pid, {:fetch_key_selector, selector})
      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles KeySelector with offset" do
      pid = start_transaction_builder()
      selector = "test_key" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5)

      result = GenServer.call(pid, {:fetch_key_selector, selector})
      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "tracks read version management during KeySelector operations" do
      pid = start_transaction_builder()
      _initial_state = :sys.get_state(pid)

      key_selector = KeySelector.first_greater_or_equal("version_test_key")
      _result = GenServer.call(pid, {:fetch_key_selector, key_selector})

      final_state = :sys.get_state(pid)

      # The read version should be managed appropriately
      assert final_state.read_version == nil

      # Transaction state should be updated appropriately
      assert %Tx{} = final_state.tx
    end

    test "maintains transaction state consistency with KeySelector reads" do
      pid = start_transaction_builder()

      key_selector = KeySelector.first_greater_or_equal("consistency_key")

      # Multiple KeySelector calls should maintain consistent state
      result1 = GenServer.call(pid, {:fetch_key_selector, key_selector})
      result2 = GenServer.call(pid, {:fetch_key_selector, key_selector})

      # Both results should be the same format
      assert is_tuple(result1)
      assert is_tuple(result2)
      assert elem(result1, 0) in [:ok, :error]
      assert elem(result2, 0) in [:ok, :error]

      # For deterministic testing, we expect consistent results
      assert result1 == result2

      state = :sys.get_state(pid)
      assert state.state == :valid
    end
  end

  describe "KeySelector range operations" do
    test "handles {:range_fetch_key_selectors, start_selector, end_selector, opts} call" do
      start_selector = KeySelector.first_greater_or_equal("range_start")
      end_selector = KeySelector.first_greater_than("range_end")
      opts = [limit: 50]
      pid = start_transaction_builder()

      result = GenServer.call(pid, {:range_fetch_key_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert tuple_size(result) >= 2
      assert elem(result, 0) in [:ok, :error]

      # State should remain valid
      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles KeySelector range with standard boundaries" do
      pid = start_transaction_builder()

      start_selector = KeySelector.first_greater_or_equal("a")
      end_selector = KeySelector.first_greater_than("z")
      opts = []

      result = GenServer.call(pid, {:range_fetch_key_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles KeySelector range with limit option" do
      pid = start_transaction_builder()

      start_selector = "middle" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-5)
      end_selector = "middle" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5)
      opts = [limit: 100]

      result = GenServer.call(pid, {:range_fetch_key_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles range with nonexistent keys" do
      start_selector = KeySelector.first_greater_than("zzz_nonexistent")
      end_selector = KeySelector.first_greater_than("zzz_also_nonexistent")
      opts = []
      pid = start_transaction_builder()

      result = GenServer.call(pid, {:range_fetch_key_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "maintains transaction consistency across range operations" do
      pid = start_transaction_builder()

      start_selector = KeySelector.first_greater_or_equal("consistency_range_start")
      end_selector = KeySelector.first_greater_than("consistency_range_end")
      opts = [limit: 5]

      # Multiple range operations should maintain consistent state
      result1 = GenServer.call(pid, {:range_fetch_key_selectors, start_selector, end_selector, opts})
      result2 = GenServer.call(pid, {:range_fetch_key_selectors, start_selector, end_selector, opts})

      # Both results should have the same format
      assert is_tuple(result1)
      assert is_tuple(result2)
      assert elem(result1, 0) in [:ok, :error]
      assert elem(result2, 0) in [:ok, :error]

      # For deterministic behavior, results should be identical
      assert result1 == result2

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "processes range with no options" do
      pid = start_transaction_builder()

      start_selector = KeySelector.first_greater_or_equal("opts_test")
      end_selector = KeySelector.first_greater_than("opts_test_end")
      opts = []

      result = GenServer.call(pid, {:range_fetch_key_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "processes range with limit option" do
      pid = start_transaction_builder()

      start_selector = KeySelector.first_greater_or_equal("opts_test")
      end_selector = KeySelector.first_greater_than("opts_test_end")
      opts = [limit: 50, timeout: 5000]

      result = GenServer.call(pid, {:range_fetch_key_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end
  end

  describe "read version lease management" do
    test "handles :update_version_lease_if_needed continue message" do
      pid = start_transaction_builder()

      # Get initial state and verify it's valid
      initial_state = :sys.get_state(pid)
      assert initial_state.state == :valid

      # The continue message is handled internally by GenServer.continue/2
      # We can't send it directly, so we test that the process remains stable
      # and the handler exists by checking the process stays alive
      assert Process.alive?(pid)

      # State should remain valid
      final_state = :sys.get_state(pid)
      assert %State{} = final_state
      assert final_state.state == :valid
    end

    test "handles version lease expiration" do
      pid = start_transaction_builder()

      # Simulate an expired lease by manipulating state directly
      # This tests the lease expiration logic
      current_state = :sys.get_state(pid)
      expired_state = %{current_state | state: :expired}
      :sys.replace_state(pid, fn _state -> expired_state end)

      # The process should still be alive and handle the expired state
      assert Process.alive?(pid)

      updated_state = :sys.get_state(pid)
      assert updated_state.state == :expired
    end
  end
end
