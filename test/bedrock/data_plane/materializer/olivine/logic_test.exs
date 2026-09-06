defmodule Bedrock.DataPlane.Materializer.Olivine.LogicTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.ReadingTestHelpers
  alias Bedrock.DataPlane.Materializer.Olivine.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.KeySelector
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  setup do
    test_dir = Path.join(System.tmp_dir!(), "olivine_logic_test_#{:rand.uniform(100_000)}")
    File.rm_rf!(test_dir)

    on_exit(fn ->
      # Try cleanup multiple times to handle WAL file locks
      Enum.reduce_while(1..5, :error, fn attempt, _acc ->
        case File.rm_rf(test_dir) do
          {:ok, _} -> {:halt, :ok}
          _error when attempt < 5 -> {:cont, :error}
          _error -> {:halt, :error}
        end
      end)
    end)

    {:ok, test_dir: test_dir}
  end

  # Helper function to create and start a test logic state
  defp create_test_state(test_dir, otp_name \\ :test_storage, id \\ "test") do
    # Suppress expected connection retry logs during logic startup
    {result, _logs} =
      with_log(fn ->
        Logic.startup(otp_name, self(), id, test_dir, pool_size: 1)
      end)

    {:ok, state} = result
    state
  end

  describe "startup/4" do
    test "creates directory and initializes state", %{test_dir: test_dir} do
      otp_name = :test_storage
      foreman = self()
      id = "test_shard"

      {result, _logs} =
        with_log(fn ->
          Logic.startup(otp_name, foreman, id, test_dir, pool_size: 1)
        end)

      assert {:ok,
              %State{
                path: ^test_dir,
                otp_name: ^otp_name,
                id: ^id,
                foreman: ^foreman,
                database: {_, _},
                index_manager: %IndexManager{}
              } = state} = result

      assert File.dir?(test_dir)
      Logic.shutdown(state)
    end

    test "handles directory creation failure" do
      invalid_path = "/invalid/read-only/path"

      result = Logic.startup(:test_storage, self(), "test", invalid_path, pool_size: 1)
      assert {:error, _posix_error} = result
    end
  end

  describe "shutdown/1" do
    test "properly shuts down all components", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      assert :ok = Logic.shutdown(state)
    end
  end

  describe "lock_for_recovery/3" do
    test "locks successfully with valid epoch", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      director = make_ref()
      epoch = 5

      assert {:ok,
              %State{
                mode: :locked,
                director: ^director,
                epoch: ^epoch
              } = locked_state} = Logic.lock_for_recovery(state, director, epoch)

      Logic.shutdown(locked_state)
    end

    test "rejects older epoch", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      state = %{state | epoch: 10}
      director = make_ref()
      old_epoch = 5

      assert {:error, :newer_epoch_exists} = Logic.lock_for_recovery(state, director, old_epoch)
      Logic.shutdown(state)
    end

    test "handles locking without active puller", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      director = make_ref()
      epoch = 1

      assert {:ok, %State{pull_task: nil} = locked_state} = Logic.lock_for_recovery(state, director, epoch)
      Logic.shutdown(locked_state)
    end
  end

  describe "unlock_after_recovery/3" do
    test "without a shard assignment the materializer unlocks static — no puller", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      locked_state = %{state | mode: :locked, epoch: 1}
      durable_version = Version.from_integer(100)

      assert {:ok, %State{mode: :running, pull_task: nil}} =
               Logic.unlock_after_recovery(locked_state, durable_version, [])

      Logic.shutdown(locked_state)
    end

    test "with a shard assignment the stream puller is installed", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      locked_state = %{state | mode: :locked, epoch: 1, shard_num: 1}
      durable_version = Version.from_integer(100)

      assert {:ok, %State{mode: :running, pull_task: %Task{}} = unlocked_state} =
               Logic.unlock_after_recovery(locked_state, durable_version, [])

      Logic.shutdown(unlocked_state)
    end
  end

  describe "get/4" do
    test "returns error for missing key in sync mode", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      assert {:error, _} = ReadingTestHelpers.get(state, "nonexistent_key", 1, [])
      Logic.shutdown(state)
    end

    test "returns immediate error with reply_fn when fetch fails", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      reply_fn = fn result -> send(self(), {:async_result, result}) end

      # Immediate errors return error, not task pid
      assert {:error, _} = ReadingTestHelpers.get(state, "async_key", 1, reply_fn: reply_fn)
      Logic.shutdown(state)
    end

    test "handles KeySelector with immediate error", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      key_selector = KeySelector.first_greater_or_equal("selector_key")
      reply_fn = fn result -> send(self(), {:selector_result, result}) end

      assert {:error, _} = ReadingTestHelpers.get(state, key_selector, 1, reply_fn: reply_fn)
      Logic.shutdown(state)
    end

    test "handles various version scenarios", %{test_dir: test_dir} do
      state = create_test_state(test_dir)

      # Test different versions that trigger error paths
      assert {:error, _} = ReadingTestHelpers.get(state, "key", 0, [])
      assert {:error, _} = ReadingTestHelpers.get(state, "key", 999_999, [])
      Logic.shutdown(state)
    end
  end

  describe "get_range/5" do
    test "executes synchronously and returns version_too_old for empty database", %{test_dir: test_dir} do
      state = create_test_state(test_dir)

      assert {:error, :version_too_old} =
               ReadingTestHelpers.get_range(state, "range_key1", "range_key3", 1, [])

      Logic.shutdown(state)
    end

    test "returns immediate error with reply_fn when range fetch fails", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      reply_fn = fn result -> send(self(), {:range_result, result}) end

      assert {:error, _} =
               ReadingTestHelpers.get_range(state, "async_range1", "async_range3", 1, reply_fn: reply_fn)

      Logic.shutdown(state)
    end

    test "respects limit parameter in options", %{test_dir: test_dir} do
      state = create_test_state(test_dir)

      assert {:error, :version_too_old} =
               ReadingTestHelpers.get_range(state, "limit_key1", "limit_key9", 1, limit: 2)

      Logic.shutdown(state)
    end
  end

  describe "stop_pulling/1" do
    test "handles state with no puller task" do
      state = %State{pull_task: nil}
      assert %State{pull_task: nil} = Logic.stop_pulling(state)
    end
  end

  describe "info/2" do
    test "returns sensible values for every supported fact", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :info_facts_test)

      assert {:ok, supported} = Logic.info(state, :supported_info)
      assert is_list(supported)
      assert Enum.all?(supported, &is_atom/1)

      # The advertised facts must at least include the core ones exercised below
      for fact <- [
            :durable_version,
            :oldest_durable_version,
            :id,
            :otp_name,
            :path,
            :kind,
            :pid,
            :key_ranges,
            :n_keys,
            :size_in_bytes,
            :utilization
          ] do
        assert fact in supported, "expected #{inspect(fact)} to be advertised in :supported_info"
      end

      zero_version = Version.zero()
      assert {:ok, ^zero_version} = Logic.info(state, :durable_version)
      assert {:ok, ^zero_version} = Logic.info(state, :oldest_durable_version)

      assert {:ok, "test"} = Logic.info(state, :id)
      assert {:ok, :info_facts_test} = Logic.info(state, :otp_name)
      assert {:ok, ^test_dir} = Logic.info(state, :path)
      assert {:ok, :materializer} = Logic.info(state, :kind)

      self_pid = self()
      assert {:ok, ^self_pid} = Logic.info(state, :pid)

      # key_ranges reports one {min, max} range per current index
      assert {:ok, [{min_key, max_key}]} = Logic.info(state, :key_ranges)
      assert is_binary(min_key) and is_binary(max_key)

      assert {:ok, n_keys} = Logic.info(state, :n_keys)
      assert is_integer(n_keys) and n_keys >= 0

      assert {:ok, size_in_bytes} = Logic.info(state, :size_in_bytes)
      assert is_integer(size_in_bytes) and size_in_bytes >= 0

      assert {:ok, utilization} = Logic.info(state, :utilization)
      assert is_number(utilization) and utilization >= 0

      Logic.shutdown(state)
    end

    test "returns error tuple for an unsupported fact", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :info_unsupported_test)

      assert {:ok, {:error, :unsupported_info}} = Logic.info(state, :not_a_real_fact)

      Logic.shutdown(state)
    end

    test "returns a map when given a list of fact names", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :info_list_test)

      assert {:ok, facts} = Logic.info(state, [:kind, :n_keys, :id, :bogus_fact])

      assert %{
               kind: :materializer,
               n_keys: 0,
               id: "test",
               bogus_fact: {:error, :unsupported_info}
             } = facts

      Logic.shutdown(state)
    end
  end

  describe "apply_transactions/2" do
    test "applies a single transaction and makes its writes readable", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :apply_single_test)

      txn = TransactionTestSupport.new_log_transaction_from_mutations([{:set, "hello", "world"}], 1)

      assert {:ok, %State{} = updated_state, version} = Logic.apply_transactions(state, [txn])
      assert version == Version.from_integer(1)
      assert updated_state.index_manager.current_version == version

      assert {:ok, "world"} = ReadingTestHelpers.get(updated_state, "hello", version)

      Logic.shutdown(updated_state)
    end

    test "applies a batch and returns the last transaction's commit version", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :apply_batch_test)

      transactions = [
        TransactionTestSupport.new_log_transaction_from_mutations([{:set, "key_a", "value_a"}], 1),
        TransactionTestSupport.new_log_transaction_from_mutations([{:set, "key_b", "value_b"}], 2),
        TransactionTestSupport.new_log_transaction_from_mutations(
          [{:set, "key_c", "value_c"}, {:set, "key_a", "value_a2"}],
          3
        )
      ]

      assert {:ok, %State{} = updated_state, version} = Logic.apply_transactions(state, transactions)
      assert version == Version.from_integer(3)

      # All writes visible at the final version, including the overwrite
      assert {:ok, "value_a2"} = ReadingTestHelpers.get(updated_state, "key_a", version)
      assert {:ok, "value_b"} = ReadingTestHelpers.get(updated_state, "key_b", version)
      assert {:ok, "value_c"} = ReadingTestHelpers.get(updated_state, "key_c", version)

      # Earlier version still sees the original value
      v1 = Version.from_integer(1)
      assert {:ok, "value_a"} = ReadingTestHelpers.get(updated_state, "key_a", v1)

      Logic.shutdown(updated_state)
    end
  end

  describe "advance_window/1" do
    test "does not evict when nothing has been applied", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :advance_noop_test)

      durable_before = Database.durable_version(state.database)

      assert {:ok, %State{} = updated_state} = Logic.advance_window(state)
      assert Database.durable_version(updated_state.database) == durable_before

      Logic.shutdown(updated_state)
    end

    test "does not evict when all versions are within the lag window", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :advance_within_window_test)

      # Both versions are less than window_lag_time_μs apart from the newest,
      # so nothing falls behind the window edge.
      txns = [
        TransactionTestSupport.new_log_transaction_from_mutations([{:set, "a", "1"}], 1),
        TransactionTestSupport.new_log_transaction_from_mutations([{:set, "b", "2"}], 2)
      ]

      {:ok, state, _version} = Logic.apply_transactions(state, txns)

      assert {:ok, %State{} = updated_state} = Logic.advance_window(state)
      assert Database.durable_version(updated_state.database) == Version.zero()

      Logic.shutdown(updated_state)
    end

    test "evicts versions older than the lag window and advances the durable version", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :advance_evict_test)

      # window_lag_time_μs defaults to 5_000_000. With the newest version at
      # 12_000_000, the window edge is 7_000_000: version 6_000_000 is evicted,
      # version 12_000_000 stays in the window.
      old_txn = TransactionTestSupport.new_log_transaction_from_mutations([{:set, "old_key", "old_value"}], 6_000_000)
      new_txn = TransactionTestSupport.new_log_transaction_from_mutations([{:set, "new_key", "new_value"}], 12_000_000)

      {:ok, state, _} = Logic.apply_transactions(state, [old_txn])
      {:ok, state, _} = Logic.apply_transactions(state, [new_txn])

      assert Database.durable_version(state.database) == Version.zero()

      assert {:ok, %State{} = updated_state} = Logic.advance_window(state)
      assert Database.durable_version(updated_state.database) == Version.from_integer(6_000_000)

      # Evicted data remains readable at the durable version from the database
      assert {:ok, "old_value"} =
               ReadingTestHelpers.get(updated_state, "old_key", Version.from_integer(6_000_000))

      Logic.shutdown(updated_state)
    end

    test "evicting a version smaller than the lag clamps the telemetry window edge", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :advance_underflow_test)

      # Eviction version 1 is smaller than window_lag_time_μs (5_000_000), so
      # computing the telemetry window edge underflows and must clamp to zero
      # rather than raising.
      tiny_txn = TransactionTestSupport.new_log_transaction_from_mutations([{:set, "tiny", "value"}], 1)
      newer_txn = TransactionTestSupport.new_log_transaction_from_mutations([{:set, "newer", "value"}], 10_000_000)

      {:ok, state, _} = Logic.apply_transactions(state, [tiny_txn])
      {:ok, state, _} = Logic.apply_transactions(state, [newer_txn])

      assert {:ok, %State{} = updated_state} = Logic.advance_window(state)
      assert Database.durable_version(updated_state.database) == Version.from_integer(1)

      Logic.shutdown(updated_state)
    end
  end

  describe "start_compaction/1" do
    test "compacts in the background and notifies the caller", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :compaction_test)

      txns = [
        TransactionTestSupport.new_log_transaction_from_mutations([{:set, "compact_a", "value_a"}], 1),
        TransactionTestSupport.new_log_transaction_from_mutations([{:set, "compact_b", "value_b"}], 2)
      ]

      {:ok, state, _version} = Logic.apply_transactions(state, txns)

      expected_durable_version = Database.durable_version(state.database)

      assert {:ok, %Task{} = task} = Logic.start_compaction(state)

      assert_receive {:compaction_ready, _data_fd, _idx_fd, data_path, idx_path, data_offset, idx_offset,
                      compacted_pages, durable_version, duration_μs, data_size_before, idx_size_before},
                     10_000

      assert durable_version == expected_durable_version
      assert is_integer(data_offset) and data_offset >= 0
      assert is_integer(idx_offset) and idx_offset > 0
      assert is_integer(duration_μs) and duration_μs >= 0
      assert is_integer(data_size_before) and data_size_before >= 0
      assert is_integer(idx_size_before) and idx_size_before >= 0
      assert is_map(compacted_pages)

      # Compact files were written alongside the originals
      assert File.exists?(to_string(data_path))
      assert File.exists?(to_string(idx_path))
      assert String.ends_with?(to_string(data_path), ".compact")
      assert String.ends_with?(to_string(idx_path), ".compact")

      assert :ok = Task.await(task, 10_000)

      # The raw fds are owned by the (now finished) task process; just remove
      # the compact files that were produced.
      File.rm(to_string(data_path))
      File.rm(to_string(idx_path))

      Logic.shutdown(state)
    end
  end

  describe "maybe_upload_snapshot/4" do
    test "is a no-op when no snapshot is configured", %{test_dir: test_dir} do
      state = create_test_state(test_dir, :snapshot_nil_test)
      assert state.snapshot == nil

      assert :ok = Logic.maybe_upload_snapshot(state, ~c"unused", ~c"unused", Version.from_integer(1))

      Logic.shutdown(state)
    end

    test "uploads a readable snapshot bundle when configured", %{test_dir: test_dir} do
      object_storage_root = Path.join(test_dir, "object_storage")
      File.mkdir_p!(object_storage_root)

      backend = ObjectStorage.backend(LocalFilesystem, root: object_storage_root)
      snapshot = Snapshot.new(backend, "logic-upload-shard")

      state = create_test_state(Path.join(test_dir, "storage"), :snapshot_upload_test)
      state = %{state | snapshot: snapshot}

      data_path = Path.join(test_dir, "upload_data")
      idx_path = Path.join(test_dir, "upload_idx")
      File.write!(data_path, "data contents")
      File.write!(idx_path, "idx contents")

      assert :ok =
               Logic.maybe_upload_snapshot(
                 state,
                 String.to_charlist(data_path),
                 String.to_charlist(idx_path),
                 Version.from_integer(42)
               )

      # Upload happens in a fire-and-forget task; poll until it lands
      assert {:ok, 42, uploaded} = await_snapshot(snapshot, 100)
      assert IO.iodata_to_binary(uploaded) == "data contents" <> "idx contents"

      Logic.shutdown(state)
    end
  end

  defp await_snapshot(_snapshot, 0), do: {:error, :timed_out}

  defp await_snapshot(snapshot, attempts_left) do
    case Snapshot.read_latest(snapshot) do
      {:ok, version, data} ->
        {:ok, version, data}

      {:error, :not_found} ->
        Process.sleep(50)
        await_snapshot(snapshot, attempts_left - 1)
    end
  end

  describe "the :epoch info fact (assignment verification, bedrock-q67.21.5)" do
    test "a never-locked worker reports nil; a locked worker reports its epoch", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:epoch_fact_test, self(), "epoch_wkr", test_dir)

      assert {:ok, %{epoch: nil}} = Logic.info(state, [:epoch])

      {:ok, locked} = Logic.lock_for_recovery(state, self(), 7)
      assert {:ok, %{epoch: 7}} = Logic.info(locked, [:epoch])
      assert :epoch in elem(Logic.info(locked, [:supported_info]), 1).supported_info

      Logic.shutdown(locked)
    end
  end
end
