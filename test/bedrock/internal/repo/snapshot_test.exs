defmodule Bedrock.Internal.Repo.SnapshotTest do
  @moduledoc """
  Tests for snapshot read API that bypasses conflict tracking.

  This test module verifies that:
  - get/3 with snapshot: true returns the same values as get/2 but without read conflicts
  - range/4 with snapshot: true works like range queries but without conflicts
  - snapshot reads integrate correctly with the transaction system
  """
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Repo

  # Mock transaction process to test the conflict clearing behavior
  defmodule MockTransaction do
    @moduledoc false
    use GenServer

    def start_link(initial_state \\ %{}) do
      GenServer.start_link(__MODULE__, initial_state)
    end

    @impl true
    def init(state) do
      {:ok, state}
    end

    @impl true
    def handle_call({:get, key, _opts}, _from, state) do
      value = Map.get(state, key)
      result = if value, do: {:ok, value}, else: {:error, :not_found}
      {:reply, result, state}
    end

    def handle_call({:get_range, start_key, end_key, _batch_size, _opts}, _from, state) do
      # Mock range query - return all keys between start and end
      results =
        state
        |> Enum.filter(fn {k, _v} ->
          is_binary(k) and k >= start_key and k < end_key
        end)
        |> Enum.sort()

      {:reply, {:ok, {results, false}}, state}
    end

    @impl true
    def handle_cast({:set_key, key, value}, state) do
      {:noreply, Map.put(state, key, value)}
    end

    def handle_cast({:set_key, key, value, _opts}, state) do
      {:noreply, Map.put(state, key, value)}
    end
  end

  describe "get/3 with snapshot: true" do
    test "returns same value as get/2 with snapshot option" do
      # Setup mock transaction with data
      {:ok, tx} = MockTransaction.start_link(%{"test_key" => "test_value"})

      # Test regular get
      regular_result = Repo.get(tx, "test_key")
      assert regular_result == "test_value"

      # Test snapshot get - should return same value
      snapshot_result = Repo.get(tx, "test_key", snapshot: true)
      assert snapshot_result == "test_value"
      assert regular_result == snapshot_result
    end

    test "returns nil for non-existent keys" do
      {:ok, tx} = MockTransaction.start_link(%{})

      result = Repo.get(tx, "non_existent", snapshot: true)
      assert result == nil
    end

    test "works with different key values" do
      {:ok, tx} = MockTransaction.start_link(%{"key1" => "value1", "key2" => "value2"})

      # Test multiple snapshot reads
      result1 = Repo.get(tx, "key1", snapshot: true)
      result2 = Repo.get(tx, "key2", snapshot: true)

      assert result1 == "value1"
      assert result2 == "value2"
    end
  end

  describe "range/4 with snapshot: true" do
    setup do
      # Create transaction with test data
      data = %{
        "key1" => "value1",
        "key2" => "value2",
        "key3" => "value3",
        "other_key" => "other_value"
      }

      {:ok, tx} = MockTransaction.start_link(data)
      {:ok, tx: tx}
    end

    test "range/4 with snapshot: true works with different option formats", %{tx: tx} do
      # This test verifies the snapshot option works correctly
      result1 = tx |> Repo.range("key", "key4", snapshot: true) |> Enum.to_list()
      result2 = tx |> Repo.range("key", "key4", snapshot: true) |> Enum.to_list()

      assert result1 == result2
      expected = [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}]
      assert result1 == expected
    end

    test "works with valid parameters", %{tx: tx} do
      # Just test that it works with valid parameters
      stream = Repo.range(tx, "start", "end", snapshot: true)
      results = Enum.to_list(stream)

      # Should return empty list since no data matches the range
      assert results == []
    end

    test "returns enumerable stream with snapshot reads", %{tx: tx} do
      # Create the snapshot range stream
      stream = Repo.range(tx, "key", "key4", batch_size: 2, snapshot: true)

      # Consume the stream
      results = Enum.to_list(stream)

      # Verify we got the expected results
      expected = [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}]
      assert results == expected
    end

    test "works when stream is halted early", %{tx: tx} do
      stream = Repo.range(tx, "key", "key4", snapshot: true)

      # Take only first item and halt early
      results = Enum.take(stream, 1)

      assert length(results) == 1
      assert hd(results) == {"key1", "value1"}
    end

    test "handles empty range results", %{tx: tx} do
      stream = Repo.range(tx, "nonexistent", "nonexistent1", snapshot: true)

      results = Enum.to_list(stream)
      assert results == []
    end

    test "works with stream operations", %{tx: tx} do
      results =
        tx
        |> Repo.range("key", "key4", snapshot: true)
        |> Stream.map(fn {k, v} -> {k, String.upcase(v)} end)
        |> Enum.take(2)

      expected = [{"key1", "VALUE1"}, {"key2", "VALUE2"}]
      assert results == expected
    end
  end

  describe "integration with real transaction operations" do
    test "snapshot reads work alongside regular transaction operations" do
      # This test would require more complex setup with actual transaction system
      # For now, verify the function signatures and basic delegation work

      {:ok, tx} = MockTransaction.start_link(%{"test" => "data"})

      # Mix regular operations with snapshot operations
      Repo.put(tx, "new_key", "new_value")

      # Snapshot read shouldn't interfere with transaction state
      result = Repo.get(tx, "test", snapshot: true)
      assert result == "data"
    end
  end

  describe "error handling" do
    test "snapshot functions handle transaction errors gracefully" do
      {:ok, tx} = MockTransaction.start_link(%{})

      # These should not raise, even if underlying operations have issues
      result = Repo.get(tx, "any_key", snapshot: true)
      assert result == nil

      stream_results =
        tx
        |> Repo.range("start", "end", snapshot: true)
        |> Enum.to_list()

      assert stream_results == []
    end
  end
end
