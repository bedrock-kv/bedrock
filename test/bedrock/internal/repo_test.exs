defmodule Bedrock.Internal.RepoSimpleTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Repo
  alias Bedrock.KeySelector

  # Mock transaction process to test the conflict clearing behavior
  defmodule MockTransaction do
    @moduledoc false
    use GenServer

    def start_link(initial_state \\ %{}) do
      GenServer.start_link(__MODULE__, initial_state)
    end

    @impl true
    def init(state) do
      {:ok, Map.put_new(state, :conflicts_cleared, false)}
    end

    @impl true
    def handle_call({:get, key}, _from, state) do
      value = Map.get(state, key)
      result = if value, do: {:ok, value}, else: {:error, :not_found}
      {:reply, result, state}
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

  # Helper functions for creating mock transaction processes
  defp spawn_transaction_mock(message, response) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, ^message} ->
          GenServer.reply(from, response)
      end
    end)
  end

  defp spawn_transaction_mock_with_rollback(message, response) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, ^message} ->
          GenServer.reply(from, response)

          receive do
            {:"$gen_cast", :rollback} -> :ok
          end
      end
    end)
  end

  defp spawn_get_mock(key, response) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, {:get, ^key, []}} ->
          GenServer.reply(from, response)
      end
    end)
  end

  defp spawn_select_mock(key_selector, response) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, {:get_key_selector, ^key_selector, []}} ->
          GenServer.reply(from, response)
      end
    end)
  end

  defp spawn_range_mock(start_key, end_key, batch_size, response) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, {:get_range, ^start_key, ^end_key, ^batch_size, []}} ->
          GenServer.reply(from, response)
      end
    end)
  end

  defp spawn_range_mock_with_limit(start_key, end_key, batch_size, limit, results) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, {:get_range, ^start_key, ^end_key, ^batch_size, [limit: ^limit]}} ->
          GenServer.reply(from, {:ok, {results, false}})
      end
    end)
  end

  defp spawn_range_mock_with_continuation(start_key, end_key, batch_size, first_batch, second_batch) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, {:get_range, ^start_key, ^end_key, ^batch_size, []}} ->
          GenServer.reply(from, {:ok, {first_batch, true}})
      end

      expected_key_after = first_batch |> hd() |> elem(0) |> Bedrock.Key.key_after()

      receive do
        {:"$gen_call", from, {:get_range, ^expected_key_after, ^end_key, ^batch_size, []}} ->
          GenServer.reply(from, {:ok, {second_batch, false}})
      end
    end)
  end

  describe "nested_transaction/2" do
    test "delegates to GenServer call and executes function" do
      txn_pid = spawn_transaction_mock(:nested_transaction, :ok)

      result = Repo.nested_transaction(txn_pid, fn _txn -> :test_result end)
      assert result == :test_result
    end

    test "handles exceptions and rolls back" do
      txn_pid = spawn_transaction_mock_with_rollback(:nested_transaction, :ok)

      assert_raise RuntimeError, "test error", fn ->
        Repo.nested_transaction(txn_pid, fn _txn ->
          raise RuntimeError, "test error"
        end)
      end
    end
  end

  describe "get/2 (no options)" do
    test "returns value when fetch succeeds" do
      txn_pid = spawn_get_mock("get_key", {:ok, "get_value"})

      assert Repo.get(txn_pid, "get_key") == "get_value"
    end

    test "returns nil when fetch returns error" do
      txn_pid = spawn_get_mock("missing_get_key", {:error, :not_found})

      assert Repo.get(txn_pid, "missing_get_key") == nil
    end
  end

  describe "get/3 with options" do
    test "returns value when key exists" do
      {:ok, tx} = MockTransaction.start_link(%{"test_key" => "test_value"})

      assert Repo.get(tx, "test_key", []) == "test_value"
      assert Repo.get(tx, "test_key", snapshot: true) == "test_value"
    end

    test "returns nil for non-existent keys" do
      {:ok, tx} = MockTransaction.start_link(%{})

      assert Repo.get(tx, "non_existent", []) == nil
      assert Repo.get(tx, "non_existent", snapshot: true) == nil
    end
  end

  describe "put/3" do
    test "sends cast message and returns transaction pid" do
      txn_pid = self()
      key = "put_key"
      value = "put_value"

      result = Repo.put(txn_pid, key, value)

      assert result == txn_pid
      assert_receive {:"$gen_cast", {:set_key, "put_key", "put_value", []}}
    end
  end

  describe "commit/2" do
    test "commits with default timeout" do
      txn_pid = self()

      spawn(fn ->
        Repo.commit(txn_pid)
      end)

      assert_receive {:"$gen_call", _from, :commit}
    end

    test "returns success result" do
      txn_pid = spawn_transaction_mock(:commit, {:ok, 999})

      assert {:ok, 999} = Repo.commit(txn_pid)
    end
  end

  describe "rollback/1" do
    test "sends cast message for rollback" do
      txn_pid = self()

      result = Repo.rollback(txn_pid)

      assert result == :ok
      assert_receive {:"$gen_cast", :rollback}
    end
  end

  describe "range/4" do
    test "creates a lazy stream that delegates to range_batch calls" do
      txn_pid =
        spawn_range_mock_with_continuation(
          "key_a",
          "key_z",
          2,
          [{"key_b", "value_b"}],
          [{"key_c", "value_c"}]
        )

      stream = Repo.range(txn_pid, "key_a", "key_z", batch_size: 2)
      results = stream |> Enum.to_list() |> List.flatten()

      assert results == [{"key_b", "value_b"}, {"key_c", "value_c"}]
    end

    test "handles empty results gracefully" do
      txn_pid = spawn_range_mock("key_a", "key_z", 10, {:ok, {[], false}})

      stream = Repo.range(txn_pid, "key_a", "key_z", batch_size: 10)
      results = Enum.to_list(stream)

      assert results == []
    end

    test "respects limit option" do
      expected_results = [{"key_b", "value_b"}, {"key_c", "value_c"}]
      txn_pid = spawn_range_mock_with_limit("key_a", "key_z", 2, 2, expected_results)

      stream = Repo.range(txn_pid, "key_a", "key_z", batch_size: 10, limit: 2)
      results = stream |> Enum.to_list() |> List.flatten()

      assert results == expected_results
    end
  end

  describe "select/2" do
    test "delegates to GenServer call with {:get_key_selector, key_selector} message" do
      txn_pid = self()
      key_selector = KeySelector.first_greater_or_equal("test_key")

      spawn(fn ->
        Repo.select(txn_pid, key_selector)
      end)

      assert_receive {:"$gen_call", _from, {:get_key_selector, ^key_selector, []}}
    end

    test "returns success result with resolved key-value pair" do
      key_selector = KeySelector.first_greater_or_equal("mykey")
      txn_pid = spawn_select_mock(key_selector, {:ok, {"resolved_key", "resolved_value"}})

      assert {:ok, {"resolved_key", "resolved_value"}} = Repo.select(txn_pid, key_selector)
    end

    test "returns error when KeySelector resolution fails" do
      key_selector = KeySelector.first_greater_than("nonexistent")
      txn_pid = spawn_select_mock(key_selector, {:error, :not_found})

      assert {:error, :not_found} = Repo.select(txn_pid, key_selector)
    end

    test "handles version errors" do
      key_selector = KeySelector.first_greater_or_equal("versioned_key")
      txn_pid = spawn_select_mock(key_selector, {:error, :version_too_old})

      assert {:error, :version_too_old} = Repo.select(txn_pid, key_selector)
    end

    test "handles clamped errors from cross-shard operations" do
      key_selector = "cross_shard" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1000)
      txn_pid = spawn_select_mock(key_selector, nil)

      assert Repo.select(txn_pid, key_selector) == nil
    end
  end
end
