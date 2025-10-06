defmodule Bedrock.Cluster.Gateway.TransactionBuilder.StateTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Transaction

  describe "struct creation and defaults" do
    test "creates state with default values" do
      assert %State{
               state: nil,
               gateway: nil,
               transaction_system_layout: nil,
               read_version: nil,
               commit_version: nil,
               stack: [],
               fastest_storage_servers: %{},
               fetch_timeout_in_ms: 50
             } = %State{}

      empty_tx = %State{}.tx

      assert {:ok, %{mutations: [], write_conflicts: [], read_conflicts: {nil, []}}} =
               empty_tx |> Tx.commit(nil) |> Transaction.decode()
    end

    test "creates state with custom values" do
      tx = Tx.set(Tx.new(), "key", "value")
      layout = %{sequencer: :test_sequencer}
      gateway_pid = self()
      servers = %{{:a, :b} => :pid}

      assert %State{
               state: :valid,
               gateway: ^gateway_pid,
               transaction_system_layout: ^layout,
               read_version: 12_345,
               commit_version: 11_111,
               tx: ^tx,
               stack: [stacked_tx],
               fastest_storage_servers: ^servers,
               fetch_timeout_in_ms: 200
             } = %State{
               state: :valid,
               gateway: gateway_pid,
               transaction_system_layout: layout,
               read_version: 12_345,
               commit_version: 11_111,
               tx: tx,
               stack: [Tx.new()],
               fastest_storage_servers: servers,
               fetch_timeout_in_ms: 200
             }

      assert {:ok, %{mutations: [], write_conflicts: [], read_conflicts: {nil, []}}} =
               stacked_tx |> Tx.commit(nil) |> Transaction.decode()
    end
  end

  describe "state transitions" do
    test "valid state transitions" do
      state = %State{state: :valid}

      assert %State{state: :committed, commit_version: 12_345} =
               %{state | state: :committed, commit_version: 12_345}

      assert %State{state: :rolled_back} = %{state | state: :rolled_back}
    end

    test "preserves other fields during state transitions" do
      gateway_pid = self()
      layout = %{test: "layout"}
      tx = Tx.set(Tx.new(), "key", "value")
      stack = [Tx.new()]
      servers = %{range: :server}

      original_state = %State{
        state: :valid,
        gateway: gateway_pid,
        transaction_system_layout: layout,
        read_version: 12_345,
        tx: tx,
        stack: stack,
        fastest_storage_servers: servers
      }

      assert %State{
               state: :committed,
               commit_version: 67_890,
               gateway: ^gateway_pid,
               transaction_system_layout: ^layout,
               read_version: 12_345,
               tx: ^tx,
               stack: ^stack,
               fastest_storage_servers: ^servers
             } = %{original_state | state: :committed, commit_version: 67_890}
    end
  end

  describe "transaction stack management" do
    test "pushes transaction to stack" do
      original_tx = Tx.set(Tx.new(), "key1", "value1")
      state = %State{tx: original_tx, stack: []}
      new_tx = Tx.set(Tx.new(), "key2", "value2")

      assert %State{tx: ^new_tx, stack: [^original_tx]} =
               %{state | tx: new_tx, stack: [original_tx]}
    end

    test "pops transaction from stack" do
      stacked_tx = Tx.set(Tx.new(), "stacked", "value")
      current_tx = Tx.set(Tx.new(), "current", "value")
      state = %State{tx: current_tx, stack: [stacked_tx]}

      assert %State{tx: ^stacked_tx, stack: []} = %{state | tx: stacked_tx, stack: []}
    end

    test "handles multiple nested transactions" do
      tx1 = Tx.set(Tx.new(), "key1", "value1")
      tx2 = Tx.set(Tx.new(), "key2", "value2")
      tx3 = Tx.set(Tx.new(), "key3", "value3")

      state = %State{tx: tx1, stack: []}
      state = %{state | tx: tx2, stack: [tx1]}

      assert %State{tx: ^tx3, stack: [^tx2, ^tx1]} =
               %{state | tx: tx3, stack: [tx2, tx1]}
    end
  end

  describe "read version management" do
    test "sets read version" do
      state = %State{read_version: nil}

      assert %State{
               read_version: 12_345
             } = %{state | read_version: 12_345}
    end

    test "handles zero and large version numbers" do
      state = %State{}

      assert %State{read_version: 0} = %{state | read_version: 0}

      large_version = 999_999_999_999
      assert %State{read_version: ^large_version} = %{state | read_version: large_version}
    end
  end

  describe "fastest storage servers management" do
    test "adds fastest storage server" do
      state = %State{fastest_storage_servers: %{}}
      range = {"a", "m"}
      server_pid = :test_pid

      assert %State{fastest_storage_servers: %{^range => ^server_pid}} = %{
               state
               | fastest_storage_servers: Map.put(state.fastest_storage_servers, range, server_pid)
             }
    end

    test "updates fastest storage servers" do
      range1 = {"a", "m"}
      range2 = {"m", :end}
      state = %State{fastest_storage_servers: %{range1 => :old_pid}}
      expected_servers = %{range1 => :new_pid, range2 => :another_pid}

      assert %State{fastest_storage_servers: ^expected_servers} = %{
               state
               | fastest_storage_servers: expected_servers
             }
    end

    test "handles complex range keys" do
      ranges = [{"", :end}, {"a", "z"}, {"key_prefix", "key_prefiy"}, {"\x00", "\xFF"}]
      servers = Enum.with_index(ranges, fn range, i -> {range, :"pid_#{i}"} end)
      expected_servers = Map.new(servers)

      assert %State{fastest_storage_servers: ^expected_servers} =
               %State{fastest_storage_servers: expected_servers}

      # Verify specific key lookups work
      assert expected_servers[{"", :end}] == :pid_0
      assert expected_servers[{"\x00", "\xFF"}] == :pid_3
    end
  end

  describe "timeout configuration" do
    test "default timeout values" do
      assert %State{
               fetch_timeout_in_ms: 50
             } = %State{}
    end

    test "custom timeout values" do
      assert %State{
               fetch_timeout_in_ms: 1000
             } = %State{
               fetch_timeout_in_ms: 1000
             }
    end

    test "preserves timeout values during other updates" do
      original_state = %State{
        fetch_timeout_in_ms: 500
      }

      tx = Tx.set(Tx.new(), "key", "value")

      assert %State{
               state: :valid,
               read_version: 12_345,
               tx: ^tx,
               fetch_timeout_in_ms: 500
             } = %{
               original_state
               | state: :valid,
                 read_version: 12_345,
                 tx: tx
             }
    end
  end

  describe "field validation and type checking" do
    test "state field accepts valid states" do
      valid_states = [:valid, :committed, :rolled_back]

      for valid_state <- valid_states do
        assert %State{state: ^valid_state} = %State{state: valid_state}
      end
    end

    test "tx field maintains Tx type" do
      assert %State{tx: %Tx{}} = %State{}

      new_tx = Tx.set(Tx.new(), "key", "value")
      assert %State{tx: %Tx{}} = %State{tx: new_tx}
    end

    test "stack field maintains list of Tx" do
      assert %State{stack: []} = %State{}

      tx1 = Tx.set(Tx.new(), "key1", "value1")
      tx2 = Tx.set(Tx.new(), "key2", "value2")

      assert %State{stack: [%Tx{}, %Tx{}]} = %State{stack: [tx1, tx2]}
    end

    test "fastest_storage_servers maintains map type" do
      assert %State{fastest_storage_servers: %{}} = %State{}

      servers = %{{"a", "z"} => :pid1, {"z", :end} => :pid2}
      assert %State{fastest_storage_servers: ^servers} = %State{fastest_storage_servers: servers}
    end
  end

  describe "integration with transaction operations" do
    test "state changes during put operations" do
      initial_state = %State{state: :valid, tx: Tx.new()}
      new_tx = Tx.set(initial_state.tx, "key", "value")

      assert %State{state: :valid, tx: ^new_tx} = %{initial_state | tx: new_tx}

      assert {:ok, %{mutations: [{:set, "key", "value"}]}} =
               new_tx |> Tx.commit(nil) |> Transaction.decode()
    end

    test "state changes during commit operations" do
      tx_with_data = Tx.set(Tx.new(), "key", "value")
      initial_state = %State{state: :valid, tx: tx_with_data}

      assert %State{
               state: :committed,
               commit_version: 12_345,
               tx: ^tx_with_data
             } = %{initial_state | state: :committed, commit_version: 12_345}
    end

    test "state changes during rollback operations" do
      current_tx = Tx.set(Tx.new(), "current", "value")
      stacked_tx = Tx.set(Tx.new(), "stacked", "value")

      initial_state = %State{
        state: :valid,
        tx: current_tx,
        stack: [stacked_tx]
      }

      assert %State{
               state: :valid,
               tx: ^stacked_tx,
               stack: []
             } = %{initial_state | tx: stacked_tx, stack: []}
    end
  end
end
