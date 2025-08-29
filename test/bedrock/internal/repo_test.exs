defmodule Bedrock.Internal.RepoSimpleTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Repo

  describe "nested_transaction/2" do
    test "delegates to GenServer call and executes function" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :nested_transaction} ->
              GenServer.reply(from, :ok)
          end
        end)

      result = Repo.nested_transaction(txn_pid, fn _txn -> :test_result end)
      assert result == :test_result
    end

    test "handles exceptions and rolls back" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :nested_transaction} ->
              GenServer.reply(from, :ok)

              receive do
                {:"$gen_cast", :rollback} -> :ok
              end
          end
        end)

      assert_raise RuntimeError, "test error", fn ->
        Repo.nested_transaction(txn_pid, fn _txn ->
          raise RuntimeError, "test error"
        end)
      end
    end
  end

  describe "fetch/2" do
    test "delegates to GenServer call with {:fetch, key} message" do
      txn_pid = self()
      key = "test_key"

      spawn(fn ->
        Repo.fetch(txn_pid, key)
      end)

      assert_receive {:"$gen_call", _from, {:fetch, "test_key"}}
    end

    test "returns success result" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, key}} ->
              GenServer.reply(from, {:ok, "value_for_#{key}"})
          end
        end)

      result = Repo.fetch(txn_pid, "mykey")
      assert result == {:ok, "value_for_mykey"}
    end
  end

  describe "fetch!/2" do
    test "returns value when fetch succeeds" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "success_key"}} ->
              GenServer.reply(from, {:ok, "success_value"})
          end
        end)

      result = Repo.fetch!(txn_pid, "success_key")
      assert result == "success_value"
    end

    test "raises exception when fetch returns error" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "error_key"}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      assert_raise RuntimeError, "Key not found: \"error_key\"", fn ->
        Repo.fetch!(txn_pid, "error_key")
      end
    end
  end

  describe "get/2" do
    test "returns value when fetch succeeds" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "get_key"}} ->
              GenServer.reply(from, {:ok, "get_value"})
          end
        end)

      result = Repo.get(txn_pid, "get_key")
      assert result == "get_value"
    end

    test "returns nil when fetch returns error" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "missing_get_key"}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      result = Repo.get(txn_pid, "missing_get_key")
      assert result == nil
    end
  end

  describe "put/3" do
    test "sends cast message and returns transaction pid" do
      txn_pid = self()
      key = "put_key"
      value = "put_value"

      result = Repo.put(txn_pid, key, value)

      assert result == txn_pid
      assert_receive {:"$gen_cast", {:put, "put_key", "put_value"}}
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
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:ok, 999})
          end
        end)

      result = Repo.commit(txn_pid)
      assert result == {:ok, 999}
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

  describe "range_fetch/4" do
    test "delegates to GenServer call with {:range_batch, start_key, end_key, batch_size, opts} message" do
      txn_pid = self()
      start_key = "key_a"
      end_key = "key_z"
      opts = [limit: 100]

      spawn(fn ->
        Repo.range_fetch(txn_pid, start_key, end_key, opts)
      end)

      assert_receive {:"$gen_call", _from, {:range_batch, "key_a", "key_z", 100, [limit: 100]}}
    end

    test "returns success result with key-value pairs" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:range_batch, "key_a", "key_z", 100, []}} ->
              GenServer.reply(from, {:ok, [{"key_b", "value_b"}, {"key_c", "value_c"}], :finished})
          end
        end)

      result = Repo.range_fetch(txn_pid, "key_a", "key_z")
      assert result == {:ok, [{"key_b", "value_b"}, {"key_c", "value_c"}]}
    end
  end

  describe "range_stream/4" do
    test "creates a lazy stream that delegates to range_batch calls" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:range_batch, "key_a", "key_z", 2, []}} ->
              # Return first batch with continuation
              GenServer.reply(from, {:ok, [{"key_b", "value_b"}], {:continue_from, "key_c"}})
          end

          receive do
            {:"$gen_call", from, {:range_batch, "key_c", "key_z", 2, []}} ->
              # Return final batch
              GenServer.reply(from, {:ok, [{"key_c", "value_c"}], :finished})
          end
        end)

      stream = Repo.range_stream(txn_pid, "key_a", "key_z", batch_size: 2)
      results = stream |> Enum.to_list() |> List.flatten()

      assert results == [{"key_b", "value_b"}, {"key_c", "value_c"}]
    end

    test "handles empty results gracefully" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:range_batch, "key_a", "key_z", 10, []}} ->
              GenServer.reply(from, {:ok, [], :finished})
          end
        end)

      stream = Repo.range_stream(txn_pid, "key_a", "key_z", batch_size: 10)
      results = Enum.to_list(stream)

      assert results == []
    end

    test "respects limit option" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:range_batch, "key_a", "key_z", 2, [limit: 2]}} ->
              GenServer.reply(from, {:ok, [{"key_b", "value_b"}, {"key_c", "value_c"}], :finished})
          end
        end)

      stream = Repo.range_stream(txn_pid, "key_a", "key_z", batch_size: 10, limit: 2)
      results = stream |> Enum.to_list() |> List.flatten()

      assert results == [{"key_b", "value_b"}, {"key_c", "value_c"}]
    end
  end
end
