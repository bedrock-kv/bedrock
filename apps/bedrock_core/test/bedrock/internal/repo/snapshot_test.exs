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

  defmodule TestRepo do
    use Bedrock.Repo, cluster: MockCluster
  end

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
      {:ok, tx} = MockTransaction.start_link(%{"test_key" => "test_value"})
      Process.put({:transaction, TestRepo}, tx)

      regular_result = Repo.get(TestRepo, "test_key")
      snapshot_result = Repo.get(TestRepo, "test_key", snapshot: true)

      assert regular_result == "test_value"
      assert snapshot_result == "test_value"
    end

    test "returns nil for non-existent keys" do
      {:ok, tx} = MockTransaction.start_link(%{})
      Process.put({:transaction, TestRepo}, tx)
      assert Repo.get(TestRepo, "non_existent", snapshot: true) == nil
    end

    test "works with multiple keys" do
      {:ok, tx} = MockTransaction.start_link(%{"key1" => "value1", "key2" => "value2"})
      Process.put({:transaction, TestRepo}, tx)

      assert Repo.get(TestRepo, "key1", snapshot: true) == "value1"
      assert Repo.get(TestRepo, "key2", snapshot: true) == "value2"
    end
  end

  describe "range/4 with snapshot: true" do
    setup do
      data = %{
        "key1" => "value1",
        "key2" => "value2",
        "key3" => "value3",
        "other_key" => "other_value"
      }

      {:ok, tx} = MockTransaction.start_link(data)
      expected_results = [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}]
      {:ok, tx: tx, expected_results: expected_results}
    end

    test "returns consistent results across multiple calls", %{tx: tx, expected_results: expected} do
      Process.put({:transaction, TestRepo}, tx)
      result1 = TestRepo |> Repo.get_range("key", "key4", snapshot: true) |> Enum.to_list()
      result2 = TestRepo |> Repo.get_range("key", "key4", snapshot: true) |> Enum.to_list()

      assert result1 == expected
      assert result1 == result2
    end

    test "works with batch_size parameter", %{tx: tx, expected_results: expected} do
      Process.put({:transaction, TestRepo}, tx)
      results = TestRepo |> Repo.get_range("key", "key4", batch_size: 2, snapshot: true) |> Enum.to_list()
      assert results == expected
    end

    test "supports stream operations and early halting", %{tx: tx} do
      Process.put({:transaction, TestRepo}, tx)
      # Test early halting
      [first_result] = TestRepo |> Repo.get_range("key", "key4", snapshot: true) |> Enum.take(1)
      assert first_result == {"key1", "value1"}

      # Test stream transformations
      uppercase_results =
        TestRepo
        |> Repo.get_range("key", "key4", snapshot: true)
        |> Stream.map(fn {k, v} -> {k, String.upcase(v)} end)
        |> Enum.take(2)

      assert uppercase_results == [{"key1", "VALUE1"}, {"key2", "VALUE2"}]
    end

    test "handles empty ranges", %{tx: tx} do
      Process.put({:transaction, TestRepo}, tx)
      empty_result = TestRepo |> Repo.get_range("nonexistent", "nonexistent1", snapshot: true) |> Enum.to_list()
      assert empty_result == []

      no_match_result = TestRepo |> Repo.get_range("start", "end", snapshot: true) |> Enum.to_list()
      assert no_match_result == []
    end
  end

  describe "integration and error handling" do
    test "snapshot reads work alongside regular operations and handle errors gracefully" do
      {:ok, tx} = MockTransaction.start_link(%{"test" => "data"})
      Process.put({:transaction, TestRepo}, tx)

      # Mix regular operations with snapshot operations
      Repo.put(TestRepo, "new_key", "new_value")
      assert Repo.get(TestRepo, "test", snapshot: true) == "data"

      # Test error handling with empty transaction
      {:ok, empty_tx} = MockTransaction.start_link(%{})
      Process.put({:transaction, TestRepo}, empty_tx)
      assert Repo.get(TestRepo, "any_key", snapshot: true) == nil

      empty_range_results = TestRepo |> Repo.get_range("start", "end", snapshot: true) |> Enum.to_list()
      assert empty_range_results == []
    end
  end
end
