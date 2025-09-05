defmodule Bedrock.Internal.RepoSimpleTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Repo
  alias Bedrock.KeySelector

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
              GenServer.reply(from, {:ok, {[{"key_b", "value_b"}, {"key_c", "value_c"}], false}})
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
              GenServer.reply(from, {:ok, {[{"key_b", "value_b"}], true}})
          end

          receive do
            {:"$gen_call", from, {:range_batch, "key_b\x00", "key_z", 2, []}} ->
              # Return final batch
              GenServer.reply(from, {:ok, {[{"key_c", "value_c"}], false}})
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
              GenServer.reply(from, {:ok, {[], false}})
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
              GenServer.reply(from, {:ok, {[{"key_b", "value_b"}, {"key_c", "value_c"}], false}})
          end
        end)

      stream = Repo.range_stream(txn_pid, "key_a", "key_z", batch_size: 10, limit: 2)
      results = stream |> Enum.to_list() |> List.flatten()

      assert results == [{"key_b", "value_b"}, {"key_c", "value_c"}]
    end
  end

  describe "fetch_key_selector/2" do
    test "delegates to GenServer call with {:fetch_key_selector, key_selector} message" do
      txn_pid = self()
      key_selector = KeySelector.first_greater_or_equal("test_key")

      spawn(fn ->
        Repo.fetch_key_selector(txn_pid, key_selector)
      end)

      assert_receive {:"$gen_call", _from, {:fetch_key_selector, ^key_selector}}
    end

    test "returns success result with resolved key-value pair" do
      key_selector = KeySelector.first_greater_or_equal("mykey")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:ok, {"resolved_key", "resolved_value"}})
          end
        end)

      result = Repo.fetch_key_selector(txn_pid, key_selector)
      assert result == {:ok, {"resolved_key", "resolved_value"}}
    end

    test "returns error when KeySelector resolution fails" do
      key_selector = KeySelector.first_greater_than("nonexistent")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      result = Repo.fetch_key_selector(txn_pid, key_selector)
      assert result == {:error, :not_found}
    end

    test "handles version errors" do
      key_selector = KeySelector.first_greater_or_equal("versioned_key")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :version_too_old})
          end
        end)

      result = Repo.fetch_key_selector(txn_pid, key_selector)
      assert result == {:error, :version_too_old}
    end

    test "handles clamped errors from cross-shard operations" do
      key_selector = "cross_shard" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1000)

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      result = Repo.fetch_key_selector(txn_pid, key_selector)
      assert result == {:error, :not_found}
    end
  end

  describe "fetch_key_selector!/2" do
    test "returns resolved key-value pair when KeySelector resolution succeeds" do
      key_selector = KeySelector.first_greater_than("success_key")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:ok, {"success_resolved_key", "success_value"}})
          end
        end)

      result = Repo.fetch_key_selector!(txn_pid, key_selector)
      assert result == {"success_resolved_key", "success_value"}
    end

    test "raises exception when KeySelector resolution returns :not_found" do
      key_selector = KeySelector.first_greater_or_equal("error_key")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      assert_raise RuntimeError, ~r/KeySelector not found/, fn ->
        Repo.fetch_key_selector!(txn_pid, key_selector)
      end
    end

    test "raises exception with appropriate message for version errors" do
      key_selector = KeySelector.last_less_or_equal("version_key")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :version_too_new})
          end
        end)

      assert_raise RuntimeError, ~r/KeySelector not found/, fn ->
        Repo.fetch_key_selector!(txn_pid, key_selector)
      end
    end

    test "raises exception for clamped cross-shard operations" do
      key_selector = "clamped_key" |> KeySelector.first_greater_or_equal() |> KeySelector.add(500)

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      assert_raise RuntimeError, ~r/KeySelector not found/, fn ->
        Repo.fetch_key_selector!(txn_pid, key_selector)
      end
    end
  end

  describe "get_key_selector/2" do
    test "returns resolved key-value pair when KeySelector resolution succeeds" do
      key_selector = KeySelector.last_less_than("get_key")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:ok, {"get_resolved_key", "get_value"}})
          end
        end)

      result = Repo.get_key_selector(txn_pid, key_selector)
      assert result == {"get_resolved_key", "get_value"}
    end

    test "returns nil when KeySelector resolution returns :not_found" do
      key_selector = KeySelector.first_greater_than("missing_get_key")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      result = Repo.get_key_selector(txn_pid, key_selector)
      assert result == nil
    end

    test "returns nil for version errors" do
      key_selector = KeySelector.last_less_or_equal("version_get_key")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :version_too_old})
          end
        end)

      result = Repo.get_key_selector(txn_pid, key_selector)
      assert result == nil
    end

    test "returns nil for clamped errors" do
      key_selector = "clamped_get" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-100)

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^key_selector}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      result = Repo.get_key_selector(txn_pid, key_selector)
      assert result == nil
    end
  end

  describe "range_fetch_key_selectors/4" do
    test "delegates to GenServer call with {:range_fetch_key_selectors, start_selector, end_selector, opts} message" do
      txn_pid = self()
      start_selector = KeySelector.first_greater_or_equal("range_a")
      end_selector = KeySelector.first_greater_than("range_z")
      opts = [limit: 100]

      spawn(fn ->
        Repo.range_fetch_key_selectors(txn_pid, start_selector, end_selector, opts)
      end)

      assert_receive {:"$gen_call", _from, {:range_fetch_key_selectors, ^start_selector, ^end_selector, ^opts}}
    end

    test "returns success result with resolved key-value pairs" do
      start_selector = KeySelector.first_greater_or_equal("key_a")
      end_selector = KeySelector.last_less_than("key_z")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:range_fetch_key_selectors, ^start_selector, ^end_selector, []}} ->
              GenServer.reply(from, {:ok, [{"key_b", "value_b"}, {"key_c", "value_c"}]})
          end
        end)

      result = Repo.range_fetch_key_selectors(txn_pid, start_selector, end_selector)
      assert result == {:ok, [{"key_b", "value_b"}, {"key_c", "value_c"}]}
    end

    test "returns error when range KeySelector resolution fails" do
      start_selector = KeySelector.first_greater_or_equal("bad_start")
      # Invalid range
      end_selector = KeySelector.first_greater_or_equal("bad_start")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:range_fetch_key_selectors, ^start_selector, ^end_selector, []}} ->
              GenServer.reply(from, {:error, :invalid_range})
          end
        end)

      result = Repo.range_fetch_key_selectors(txn_pid, start_selector, end_selector)
      assert result == {:error, :invalid_range}
    end

    test "handles clamped errors for cross-shard ranges" do
      start_selector = KeySelector.first_greater_or_equal("shard1_key")
      end_selector = KeySelector.first_greater_or_equal("shard2_key")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:range_fetch_key_selectors, ^start_selector, ^end_selector, []}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      result = Repo.range_fetch_key_selectors(txn_pid, start_selector, end_selector)
      assert result == {:error, :not_found}
    end

    test "passes through options correctly" do
      start_selector = KeySelector.first_greater_or_equal("opt_start")
      end_selector = KeySelector.first_greater_than("opt_end")
      opts = [limit: 50, timeout: 10_000]

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:range_fetch_key_selectors, ^start_selector, ^end_selector, ^opts}} ->
              GenServer.reply(from, {:ok, [{"opt_key", "opt_value"}]})
          end
        end)

      result = Repo.range_fetch_key_selectors(txn_pid, start_selector, end_selector, opts)
      assert result == {:ok, [{"opt_key", "opt_value"}]}
    end
  end

  describe "range_stream_key_selectors/4" do
    test "resolves KeySelectors first, then delegates to range_stream" do
      start_selector = KeySelector.first_greater_or_equal("stream_a")
      end_selector = KeySelector.last_less_or_equal("stream_z")

      # Mock the KeySelector resolution calls
      txn_pid =
        spawn(fn ->
          # First call to resolve start_selector
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^start_selector}} ->
              GenServer.reply(from, {:ok, {"resolved_stream_a", "start_value"}})
          end

          # Second call to resolve end_selector
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^end_selector}} ->
              GenServer.reply(from, {:ok, {"resolved_stream_z", "end_value"}})
          end

          # Third call for the actual range_batch operation
          receive do
            {:"$gen_call", from, {:range_batch, "resolved_stream_a", "resolved_stream_z", 100, []}} ->
              GenServer.reply(from, {:ok, {[{"stream_b", "value_b"}], false}})
          end
        end)

      stream = Repo.range_stream_key_selectors(txn_pid, start_selector, end_selector)
      results = stream |> Enum.to_list() |> List.flatten()

      assert results == [{"stream_b", "value_b"}]
    end

    test "raises exception when KeySelector resolution fails" do
      start_selector = KeySelector.first_greater_or_equal("fail_start")
      end_selector = KeySelector.first_greater_than("fail_end")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^start_selector}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      assert_raise RuntimeError, "Failed to resolve KeySelectors for streaming", fn ->
        Repo.range_stream_key_selectors(txn_pid, start_selector, end_selector)
      end
    end

    test "raises exception when end_selector resolution fails" do
      start_selector = KeySelector.first_greater_or_equal("good_start")
      end_selector = KeySelector.first_greater_than("bad_end")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^start_selector}} ->
              GenServer.reply(from, {:ok, {"resolved_good_start", "start_value"}})
          end

          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^end_selector}} ->
              GenServer.reply(from, {:error, :version_too_new})
          end
        end)

      assert_raise RuntimeError, "Failed to resolve KeySelectors for streaming", fn ->
        Repo.range_stream_key_selectors(txn_pid, start_selector, end_selector)
      end
    end

    test "passes through options to underlying range_stream" do
      start_selector = KeySelector.first_greater_or_equal("opts_start")
      end_selector = KeySelector.first_greater_than("opts_end")
      opts = [batch_size: 5, limit: 20]

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^start_selector}} ->
              GenServer.reply(from, {:ok, {"opts_start_resolved", "start_val"}})
          end

          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^end_selector}} ->
              GenServer.reply(from, {:ok, {"opts_end_resolved", "end_val"}})
          end

          receive do
            {:"$gen_call", from, {:range_batch, "opts_start_resolved", "opts_end_resolved", 5, [limit: 20]}} ->
              GenServer.reply(from, {:ok, {[{"opts_result", "opts_value"}], false}})
          end
        end)

      stream = Repo.range_stream_key_selectors(txn_pid, start_selector, end_selector, opts)
      results = stream |> Enum.to_list() |> List.flatten()

      assert results == [{"opts_result", "opts_value"}]
    end

    test "handles complex KeySelector types" do
      # Test with different KeySelector construction methods
      start_selector = "complex" |> KeySelector.first_greater_or_equal() |> KeySelector.add(2)
      end_selector = KeySelector.last_less_than("complex_z")

      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^start_selector}} ->
              GenServer.reply(from, {:ok, {"complex_resolved", "complex_start"}})
          end

          receive do
            {:"$gen_call", from, {:fetch_key_selector, ^end_selector}} ->
              GenServer.reply(from, {:ok, {"complex_z_resolved", "complex_end"}})
          end

          receive do
            {:"$gen_call", from, {:range_batch, "complex_resolved", "complex_z_resolved", 100, []}} ->
              GenServer.reply(from, {:ok, {[{"complex_middle", "complex_value"}], false}})
          end
        end)

      stream = Repo.range_stream_key_selectors(txn_pid, start_selector, end_selector)
      results = stream |> Enum.to_list() |> List.flatten()

      assert results == [{"complex_middle", "complex_value"}]
    end
  end
end
