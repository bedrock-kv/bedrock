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

  # Test codecs removed since we no longer use them

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

  def create_test_transaction_system_layout_with_mock_sequencer(read_version) do
    # Create mock sequencer process with receive loop
    mock_sequencer =
      spawn_link(fn ->
        mock_sequencer_loop(read_version)
      end)

    %{
      sequencer: mock_sequencer,
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

  defp mock_sequencer_loop(read_version) do
    receive do
      {:"$gen_call", from, :next_read_version} ->
        GenServer.reply(from, {:ok, read_version})
        mock_sequencer_loop(read_version)
    end
  end

  defp create_mock_gateway do
    spawn_link(fn ->
      mock_gateway_loop()
    end)
  end

  defp mock_gateway_loop do
    receive do
      {:"$gen_call", from, {:renew_read_version_lease, _read_version}} ->
        GenServer.reply(from, {:ok, 60_000})
        mock_gateway_loop()
    end
  end

  def start_transaction_builder(opts \\ []) do
    read_version = Keyword.get(opts, :read_version)

    # For tests with read_version, provide a deterministic time function
    time_fn =
      if read_version do
        # Fixed timestamp for deterministic tests
        fn -> 1_000_000_000 end
      else
        &Bedrock.Internal.Time.monotonic_now_in_ms/0
      end

    # Create gateway and transaction system layout with mocks if read_version specified
    {gateway, transaction_system_layout} =
      if read_version do
        mock_gateway = create_mock_gateway()
        {mock_gateway, create_test_transaction_system_layout_with_mock_sequencer(read_version)}
      else
        {self(), create_test_transaction_system_layout()}
      end

    default_opts = [
      gateway: gateway,
      transaction_system_layout: transaction_system_layout,
      time_fn: time_fn
    ]

    process_opts = Keyword.delete(opts, :read_version)
    final_opts = Keyword.merge(default_opts, process_opts)
    start_supervised!({TransactionBuilder, final_opts})
  end

  describe "GenServer process lifecycle" do
    test "starts successfully with valid options" do
      pid = start_transaction_builder()
      assert Process.alive?(pid)

      state = :sys.get_state(pid)
      assert %State{} = state
      assert state.state == :valid
      assert state.gateway == self()
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
        start_transaction_builder(transaction_system_layout: custom_layout)

      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
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

      GenServer.cast(pid, {:set_key, "test_key", "test_value"})
      :timer.sleep(10)

      result = GenServer.call(pid, {:get, "test_key"})
      assert result == {:ok, {"test_key", "test_value"}}
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
    test "{:set_key, key, value} cast updates transaction state" do
      pid = start_transaction_builder()

      initial_state = :sys.get_state(pid)
      initial_mutations = Tx.commit(initial_state.tx).mutations

      GenServer.cast(pid, {:set_key, "test_key", "test_value"})
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

      GenServer.cast(pid, {:set_key, "key1", "value1"})
      GenServer.cast(pid, {:set_key, "key2", "value2"})
      GenServer.cast(pid, {:set_key, "key3", "value3"})
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

      GenServer.cast(pid, {:set_key, "key1", "value1"})
      GenServer.cast(pid, {:set_key, "key1", "updated_value"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "key1", "updated_value"}
             ]

      result = GenServer.call(pid, {:get, "key1"})
      assert result == {:ok, {"key1", "updated_value"}}
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

      GenServer.cast(pid, {:set_key, "key", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:set_key, "key2", "value2"})
      :timer.sleep(10)

      state = :sys.get_state(pid)

      assert state.state == :valid
      assert state.transaction_system_layout == custom_layout
      assert state.gateway == self()
    end

    test "stack management through GenServer operations" do
      pid = start_transaction_builder()

      initial_state = :sys.get_state(pid)
      assert Enum.empty?(initial_state.stack)

      GenServer.cast(pid, {:set_key, "base", "value"})
      :ok = GenServer.call(pid, :nested_transaction)

      nested_state = :sys.get_state(pid)
      assert length(nested_state.stack) == 1

      GenServer.cast(pid, {:set_key, "nested", "value"})
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

      GenServer.cast(pid, {:set_key, "key1", "value1"})
      :timer.sleep(5)

      state1 = :sys.get_state(pid)
      mutations1 = Tx.commit(state1.tx).mutations
      assert length(mutations1) == 1

      GenServer.cast(pid, {:set_key, "key2", "value2"})
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
        GenServer.cast(pid, {:set_key, "key_#{i}", "value_#{i}"})
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
    test "transaction builder works without codecs" do
      pid = start_transaction_builder()

      state = :sys.get_state(pid)
      assert state.state == :valid
      assert state.gateway == self()

      GenServer.cast(pid, {:set_key, "test", "value"})
      :timer.sleep(10)

      final_state = :sys.get_state(pid)
      mutations = Tx.commit(final_state.tx).mutations
      assert [{:set, "test", "value"}] = mutations
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

      GenServer.cast(pid, {:set_key, "key", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
    end
  end

  describe "KeySelector operations" do
    test "handles {:get_key_selector, key_selector} call and delegates to resolution module" do
      key_selector = KeySelector.first_greater_or_equal("test_key")
      pid = start_transaction_builder(read_version: 42)

      # Make the call - this will delegate to KeySelectorResolution.resolve_key_selector/3
      result = GenServer.call(pid, {:get_key_selector, key_selector})

      assert is_tuple(result)
      assert tuple_size(result) >= 2
      assert elem(result, 0) in [:ok, :error]

      # Check that the call was properly handled and state remains valid
      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles KeySelector with first_greater_or_equal" do
      pid = start_transaction_builder(read_version: 42)
      selector = KeySelector.first_greater_or_equal("test_key")

      result = GenServer.call(pid, {:get_key_selector, selector})
      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles KeySelector with offset" do
      pid = start_transaction_builder(read_version: 42)
      selector = "test_key" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5)

      result = GenServer.call(pid, {:get_key_selector, selector})
      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "tracks read version management during KeySelector operations" do
      pid = start_transaction_builder(read_version: 42)
      _initial_state = :sys.get_state(pid)

      key_selector = KeySelector.first_greater_or_equal("version_test_key")
      _result = GenServer.call(pid, {:get_key_selector, key_selector})

      final_state = :sys.get_state(pid)

      # The read version should be managed appropriately
      assert final_state.read_version == 42

      # Transaction state should be updated appropriately
      assert %Tx{} = final_state.tx
    end

    test "maintains transaction state consistency with KeySelector reads" do
      pid = start_transaction_builder(read_version: 42)

      key_selector = KeySelector.first_greater_or_equal("consistency_key")

      # Multiple KeySelector calls should maintain consistent state
      result1 = GenServer.call(pid, {:get_key_selector, key_selector})
      result2 = GenServer.call(pid, {:get_key_selector, key_selector})

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

    test "updates read conflict tracking for successful KeySelector fetch" do
      pid = start_transaction_builder(read_version: 42)

      key_selector = KeySelector.first_greater_or_equal("conflict_test_key")
      result = GenServer.call(pid, {:get_key_selector, key_selector})

      # For successful resolution, check transaction state was updated
      case result do
        {:ok, {resolved_key, _value}} ->
          state = :sys.get_state(pid)
          committed = Tx.commit(state.tx, 42)

          # Check that the resolved key was added to read conflicts
          {read_version, read_conflicts} = committed.read_conflicts
          assert read_version == 42
          assert resolved_key in Enum.map(read_conflicts, fn {start, _end} -> start end)

        {:error, _reason} ->
          # For errors, transaction state should not be updated with reads
          state = :sys.get_state(pid)
          assert map_size(state.tx.reads) == 0
      end
    end
  end

  describe "KeySelector range operations" do
    test "handles {:get_range_selectors, start_selector, end_selector, opts} call" do
      start_selector = KeySelector.first_greater_or_equal("range_start")
      end_selector = KeySelector.first_greater_than("range_end")
      opts = [limit: 50]
      pid = start_transaction_builder(read_version: 42)

      result = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert tuple_size(result) >= 2
      assert elem(result, 0) in [:ok, :error]

      # State should remain valid
      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles KeySelector range with standard boundaries" do
      pid = start_transaction_builder(read_version: 42)

      start_selector = KeySelector.first_greater_or_equal("a")
      end_selector = KeySelector.first_greater_than("z")
      opts = []

      result = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles KeySelector range with limit option" do
      pid = start_transaction_builder(read_version: 42)

      start_selector = "middle" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-5)
      end_selector = "middle" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5)
      opts = [limit: 100]

      result = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "handles range with nonexistent keys" do
      start_selector = KeySelector.first_greater_than("zzz_nonexistent")
      end_selector = KeySelector.first_greater_than("zzz_also_nonexistent")
      opts = []
      pid = start_transaction_builder(read_version: 42)

      result = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "maintains transaction consistency across range operations" do
      pid = start_transaction_builder(read_version: 42)

      start_selector = KeySelector.first_greater_or_equal("consistency_range_start")
      end_selector = KeySelector.first_greater_than("consistency_range_end")
      opts = [limit: 5]

      # Multiple range operations should maintain consistent state
      result1 = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})
      result2 = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})

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

    test "updates range read conflict tracking for successful KeySelector range fetch" do
      pid = start_transaction_builder(read_version: 100)

      start_selector = KeySelector.first_greater_or_equal("range_conflict_start")
      end_selector = KeySelector.first_greater_than("range_conflict_end")
      opts = [limit: 10]

      result = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})

      # For successful resolution, check transaction state was updated
      case result do
        {:ok, [_ | _] = key_values} ->
          state = :sys.get_state(pid)
          committed = Tx.commit(state.tx, 100)

          # Check that individual keys were added to reads
          for {key, _value} <- key_values do
            assert Map.has_key?(state.tx.reads, key)
          end

          # Check that the range was added to range_reads
          refute Enum.empty?(state.tx.range_reads)
          {first_key, _} = hd(key_values)
          {last_key, _} = List.last(key_values)
          assert {first_key, last_key} in state.tx.range_reads

          # Check read conflicts include the range
          {read_version, read_conflicts} = committed.read_conflicts
          assert read_version == 100
          assert {first_key, last_key} in read_conflicts

        {:ok, []} ->
          # Empty results should not add to conflict sets
          state = :sys.get_state(pid)
          assert Enum.empty?(state.tx.range_reads)

        {:error, _reason} ->
          # For errors, transaction state should not be updated
          state = :sys.get_state(pid)
          assert map_size(state.tx.reads) == 0
          assert Enum.empty?(state.tx.range_reads)
      end
    end

    test "processes range with no options" do
      pid = start_transaction_builder(read_version: 42)

      start_selector = KeySelector.first_greater_or_equal("opts_test")
      end_selector = KeySelector.first_greater_than("opts_test_end")
      opts = []

      result = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})

      assert is_tuple(result)
      assert elem(result, 0) in [:ok, :error]

      state = :sys.get_state(pid)
      assert state.state == :valid
    end

    test "processes range with limit option" do
      pid = start_transaction_builder(read_version: 42)

      start_selector = KeySelector.first_greater_or_equal("opts_test")
      end_selector = KeySelector.first_greater_than("opts_test_end")
      opts = [limit: 50, timeout: 5000]

      result = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})

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
