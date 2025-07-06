defmodule Bedrock.DataPlane.CommitProxy.LayoutExtractionTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Server
  alias Bedrock.DataPlane.CommitProxy.State

  describe "maybe_update_layout_from_transaction/2" do
    test "extracts transaction system layout from system transaction" do
      # Create a mock transaction system layout
      layout = %{
        id: 123,
        director: self(),
        sequencer: spawn(fn -> :ok end),
        proxies: [],
        resolvers: [],
        logs: %{},
        storage_teams: [],
        services: %{}
      }

      # Create a system transaction with the layout
      system_transaction = {
        # reads
        nil,
        %{
          "\xff/system/config" => :erlang.term_to_binary({1, %{}}),
          "\xff/system/transaction_system_layout" => :erlang.term_to_binary(layout)
        }
      }

      # Create initial state without layout
      initial_state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 1,
        max_per_batch: 10,
        transaction_system_layout: nil
      }

      # Call the private function using :erlang.apply
      updated_state =
        :erlang.apply(Server, :maybe_update_layout_from_transaction, [
          initial_state,
          system_transaction
        ])

      # Verify the layout was extracted and set
      assert updated_state.transaction_system_layout == layout
    end

    test "leaves state unchanged for regular transactions" do
      # Create a regular transaction without layout
      regular_transaction = {
        # reads
        nil,
        %{
          "user/123" => "some_value"
        }
      }

      # Create initial state without layout
      initial_state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 1,
        max_per_batch: 10,
        transaction_system_layout: nil
      }

      # Call the private function
      updated_state =
        :erlang.apply(Server, :maybe_update_layout_from_transaction, [
          initial_state,
          regular_transaction
        ])

      # Verify the state is unchanged
      assert updated_state == initial_state
    end

    test "handles malformed transactions gracefully" do
      # Create malformed transaction
      malformed_transaction = "not_a_tuple"

      # Create initial state
      initial_state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 1,
        max_per_batch: 10,
        transaction_system_layout: nil
      }

      # Call the private function
      updated_state =
        :erlang.apply(Server, :maybe_update_layout_from_transaction, [
          initial_state,
          malformed_transaction
        ])

      # Verify the state is unchanged
      assert updated_state == initial_state
    end

    test "preserves existing layout when transaction has no layout" do
      # Create existing layout
      existing_layout = %{id: 456, director: self()}

      # Create transaction without layout
      transaction = {nil, %{"key" => "value"}}

      # Create initial state with existing layout
      initial_state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 1,
        max_per_batch: 10,
        transaction_system_layout: existing_layout
      }

      # Call the private function
      updated_state =
        :erlang.apply(Server, :maybe_update_layout_from_transaction, [initial_state, transaction])

      # Verify the existing layout is preserved
      assert updated_state.transaction_system_layout == existing_layout
    end
  end
end
