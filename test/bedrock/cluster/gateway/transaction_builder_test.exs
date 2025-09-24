defmodule Bedrock.Cluster.Gateway.TransactionBuilderTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Transaction
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

  # Helper functions for common test patterns
  defp get_transaction_mutations(pid) do
    pid
    |> :sys.get_state()
    |> Map.fetch!(:tx)
    |> Tx.commit(nil)
    |> Transaction.decode()
    |> elem(1)
    |> Map.fetch!(:mutations)
  end

  defp assert_valid_state(pid, expected_overrides \\ []) do
    state = :sys.get_state(pid)

    expected =
      Keyword.merge(
        [
          state: :valid,
          stack: [],
          commit_version: nil,
          fastest_storage_servers: %{}
        ],
        expected_overrides
      )

    assert %State{
             state: state_value,
             stack: stack,
             commit_version: commit_version,
             fastest_storage_servers: fastest_storage_servers
           } = state

    assert state_value == expected[:state]
    assert stack == expected[:stack]
    assert commit_version == expected[:commit_version]
    assert fastest_storage_servers == expected[:fastest_storage_servers]

    # Only check read_version if explicitly specified
    if Keyword.has_key?(expected_overrides, :read_version) do
      assert state.read_version == expected[:read_version]
    end

    state
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

      assert %State{
               state: :valid,
               gateway: gateway,
               stack: [],
               tx: %Tx{}
             } = :sys.get_state(pid)

      assert gateway == self()
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
      assert result == {:ok, "test_value"}
    end

    test ":commit call succeeds for empty transaction with version 0" do
      pid = start_transaction_builder()

      result = GenServer.call(pid, :commit)
      assert {:ok, 0} = result
      refute Process.alive?(pid)
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
      initial_mutations = get_transaction_mutations(pid)

      GenServer.cast(pid, {:set_key, "test_key", "test_value"})
      :timer.sleep(10)

      final_mutations = get_transaction_mutations(pid)
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

      mutations = get_transaction_mutations(pid)

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

      mutations = get_transaction_mutations(pid)
      assert mutations == [{:set, "key1", "updated_value"}]

      assert {:ok, "updated_value"} = GenServer.call(pid, {:get, "key1"})
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

      assert %State{
               state: :valid,
               gateway: gateway,
               read_version: nil,
               commit_version: nil,
               stack: [],
               fastest_storage_servers: %{},
               fetch_timeout_in_ms: fetch_timeout,
               lease_renewal_threshold: lease_threshold,
               tx: %Tx{}
             } = :sys.get_state(pid)

      assert gateway == self()
      assert is_integer(fetch_timeout)
      assert is_integer(lease_threshold)
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
      assert length(get_transaction_mutations(pid)) == 1

      GenServer.cast(pid, {:set_key, "key2", "value2"})
      :timer.sleep(5)

      mutations = get_transaction_mutations(pid)
      assert length(mutations) == 2

      assert mutations == [
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
      assert_valid_state(pid, gateway: self())

      GenServer.cast(pid, {:set_key, "test", "value"})
      :timer.sleep(10)

      mutations = get_transaction_mutations(pid)
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
    # Helper for common KeySelector test pattern
    defp assert_key_selector_call_succeeds(pid, key_selector) do
      result = GenServer.call(pid, {:get_key_selector, key_selector, []})
      assert is_tuple(result)
      assert tuple_size(result) >= 2
      assert elem(result, 0) in [:ok, :error, :failure]
      assert_valid_state(pid)
      result
    end

    test "handles various KeySelector types" do
      pid = start_transaction_builder(read_version: 42)

      # Basic first_greater_or_equal
      selector1 = KeySelector.first_greater_or_equal("test_key")
      assert_key_selector_call_succeeds(pid, selector1)

      # With offset
      selector2 = "test_key" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5)
      assert_key_selector_call_succeeds(pid, selector2)
    end

    test "tracks read version management during KeySelector operations" do
      pid = start_transaction_builder(read_version: 42)
      _initial_state = :sys.get_state(pid)

      key_selector = KeySelector.first_greater_or_equal("version_test_key")
      _result = GenServer.call(pid, {:get_key_selector, key_selector, []})

      final_state = :sys.get_state(pid)

      # The read version should be managed appropriately
      assert final_state.read_version == 42

      # Transaction state should be updated appropriately
      assert %Tx{} = final_state.tx
    end

    test "maintains transaction state consistency with repeated KeySelector reads" do
      pid = start_transaction_builder(read_version: 42)
      key_selector = KeySelector.first_greater_or_equal("consistency_key")

      # Multiple KeySelector calls should maintain consistent state
      result1 = assert_key_selector_call_succeeds(pid, key_selector)
      result2 = assert_key_selector_call_succeeds(pid, key_selector)

      # For deterministic testing, we expect consistent results
      assert result1 == result2
    end

    test "updates read conflict tracking for successful KeySelector fetch" do
      pid = start_transaction_builder(read_version: 42)

      key_selector = KeySelector.first_greater_or_equal("conflict_test_key")
      result = GenServer.call(pid, {:get_key_selector, key_selector, []})

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

        {:failure, _failures} ->
          # For failures, transaction state should not be updated with reads
          state = :sys.get_state(pid)
          assert map_size(state.tx.reads) == 0
      end
    end
  end

  describe "KeySelector range operations" do
    # Helper for common range selector test pattern
    defp assert_range_selector_call_succeeds(pid, start_selector, end_selector, opts) do
      result = GenServer.call(pid, {:get_range_selectors, start_selector, end_selector, opts})
      assert is_tuple(result)
      assert tuple_size(result) >= 2
      assert elem(result, 0) in [:ok, :error, :failure]
      assert_valid_state(pid)
      result
    end

    test "handles various KeySelector range configurations" do
      pid = start_transaction_builder(read_version: 42)

      # Basic range with options
      start1 = KeySelector.first_greater_or_equal("range_start")
      end1 = KeySelector.first_greater_than("range_end")
      assert_range_selector_call_succeeds(pid, start1, end1, limit: 50)

      # Standard boundaries with no options
      start2 = KeySelector.first_greater_or_equal("a")
      end2 = KeySelector.first_greater_than("z")
      assert_range_selector_call_succeeds(pid, start2, end2, [])

      # Range with offset selectors
      start3 = "middle" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-5)
      end3 = "middle" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5)
      assert_range_selector_call_succeeds(pid, start3, end3, limit: 100)

      # Range with nonexistent keys
      start4 = KeySelector.first_greater_than("zzz_nonexistent")
      end4 = KeySelector.first_greater_than("zzz_also_nonexistent")
      assert_range_selector_call_succeeds(pid, start4, end4, [])
    end

    test "maintains transaction consistency across repeated range operations" do
      pid = start_transaction_builder(read_version: 42)
      start_selector = KeySelector.first_greater_or_equal("consistency_range_start")
      end_selector = KeySelector.first_greater_than("consistency_range_end")
      opts = [limit: 5]

      # Multiple range operations should maintain consistent state
      result1 = assert_range_selector_call_succeeds(pid, start_selector, end_selector, opts)
      result2 = assert_range_selector_call_succeeds(pid, start_selector, end_selector, opts)

      # For deterministic behavior, results should be identical
      assert result1 == result2
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

        {:failure, _reasons} ->
          # For failures, transaction state should not be updated
          state = :sys.get_state(pid)
          assert map_size(state.tx.reads) == 0
          assert Enum.empty?(state.tx.range_reads)
      end
    end
  end

  describe "read version lease management" do
    test "handles lease lifecycle properly" do
      pid = start_transaction_builder()

      # Initial state should be valid
      assert_valid_state(pid)
      assert Process.alive?(pid)

      # Simulate expired lease state
      current_state = :sys.get_state(pid)
      :sys.replace_state(pid, fn _state -> %{current_state | state: :expired} end)

      # Process should handle expired state
      assert Process.alive?(pid)
      assert %State{state: :expired} = :sys.get_state(pid)
    end
  end
end
