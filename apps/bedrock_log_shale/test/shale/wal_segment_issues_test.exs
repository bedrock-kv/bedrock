defmodule Bedrock.DataPlane.Log.Shale.WalSegmentIssuesTest do
  use ExUnit.Case, async: false

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Log.Shale.Pulling
  alias Bedrock.DataPlane.Log.Shale.Pushing
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @moduletag :tmp_dir

  describe "segment management issues" do
    test "segments should rotate after reasonable number of transactions", %{tmp_dir: tmp_dir} do
      # Start a real Shale server with small segment size (10KB) to trigger rotation quickly
      {:ok, segment_recycler} =
        SegmentRecycler.start_link(
          path: tmp_dir,
          min_available: 2,
          max_available: 3,
          # 10KB segments
          segment_size: 10 * 1024
        )

      # Create initial state with the recycler
      state = %{
        create_basic_state()
        | path: tmp_dir,
          segment_recycler: segment_recycler,
          # Set to running so push operations work
          mode: :running
      }

      # Create ~3KB transactions to fill segments quickly
      # 3KB of data
      large_data = String.duplicate("x", 3 * 1024)

      # Track segment count changes as we push transactions
      # Only need ~10 transactions to fill multiple 10KB segments
      {final_state, segment_counts} =
        Enum.reduce(1..10, {state, []}, fn i, {current_state, counts} ->
          expected_version = current_state.last_version
          transaction = TransactionTestSupport.new_log_transaction(i, %{"data" => large_data})

          new_state = assert_push_success(current_state, expected_version, transaction)
          segment_count = length([new_state.active_segment | new_state.segments])
          {new_state, [segment_count | counts]}
        end)

      unique_counts = Enum.uniq(segment_counts)

      assert length(unique_counts) > 1, """
      Expected segment rotation with 3KB transactions in 10KB segments, but count never changed.
      Segment counts observed: #{inspect(Enum.reverse(segment_counts))}
      This indicates segments are not rotating when they become full.
      Final state has #{length([final_state.active_segment | final_state.segments])} segments.
      """

      # Verify we have multiple segments as expected
      assert length([final_state.active_segment | final_state.segments]) >= 3,
             "Expected at least 3 segments after 10 large transactions, got #{length([final_state.active_segment | final_state.segments])}"

      # Clean up
      GenServer.stop(segment_recycler)
    end

    test "transaction stream should handle exact version matches correctly" do
      # Create mock transactions with specific versions
      [v1, v2, v3] = versions([100, 200, 300])
      state = create_test_state_with_versions([v1, v2, v3])

      # Test pulling from exact version match - should get transactions AFTER v2 (i.e., just v3)
      assert_pull_success(state, v2, 1, "This indicates exact version boundary logic is wrong.")
    end

    test "transaction stream should return empty when requesting from last version" do
      [v1, v2, v3] = versions([100, 200, 300])
      state = create_test_state_with_versions([v1, v2, v3])

      # Pull from the last version - should trigger waiting behavior
      assert_pull_waiting(state, v3, """
      Expected waiting behavior when pulling from last version #{inspect(v3)},
      but got transactions instead.
      """)
    end

    test "from_segments handles large gaps in versions correctly" do
      # Simulate the issue we saw: large version gaps within single segment
      test_versions = versions([1_000_000, 2_000_000, 5_000_000])
      state = create_test_state_with_versions(test_versions)
      segments = [state.active_segment | state.segments]
      middle_version = Version.from_integer(2_000_000)

      # Test pulling from middle version
      assert {:ok, stream} = TransactionStreams.from_segments(segments, middle_version)
      transactions = Enum.take(stream, 10)

      assert length(transactions) > 0, """
      Expected transactions after version #{inspect(middle_version)} with large version gaps,
      but got empty stream. This indicates from_segments can't handle large version ranges.
      """
    end

    test "segment loading includes all necessary segments for version range" do
      # Create segments with actual transactions spanning different version ranges
      segments = [
        # min_version = 1
        create_mock_segment_with_versions(versions([1, 50, 100])),
        # min_version = 101
        create_mock_segment_with_versions(versions([101, 150, 200])),
        # min_version = 201
        create_mock_segment_with_versions(versions([201, 250, 300]))
      ]

      state = %{create_basic_state() | active_segment: hd(segments), segments: tl(segments)}
      target_version = Version.from_integer(250)

      # Request transactions up to version 250 - should load multiple segments
      assert {:ok, loaded_segments} =
               Pulling.ensure_necessary_segments_are_loaded(target_version, [state.active_segment | state.segments])

      assert length(loaded_segments) >= 2, """
      Expected multiple segments loaded for version range up to #{inspect(target_version)},
      but only got #{length(loaded_segments)} segments. This indicates segment loading
      is not including all necessary segments for the requested range.
      """
    end
  end

  # Helper functions to create test data

  defp create_test_state_with_versions(versions) do
    # Create a mock state with an active segment containing transactions with these versions
    basic_state = create_basic_state()
    mock_segment = create_mock_segment_with_versions(versions)
    last_version = List.last(versions) || Version.zero()

    %{basic_state | active_segment: mock_segment, last_version: last_version, segments: []}
  end

  # Helper to create versions from integers
  defp versions(integers) when is_list(integers) do
    Enum.map(integers, &Version.from_integer/1)
  end

  # Helper to assert successful pull with expected transaction count
  defp assert_pull_success(state, version, expected_count, context_msg) do
    assert {:ok, _state, transactions} = Pulling.pull(state, version)

    assert length(transactions) == expected_count, """
    #{context_msg}
    Expected #{expected_count} transactions after version #{inspect(version)}, got #{length(transactions)}.
    Transactions: #{inspect(transactions)}
    """

    transactions
  end

  # Helper to assert pull waiting state
  defp assert_pull_waiting(state, version, context_msg) do
    assert {:waiting_for, ^version} = Pulling.pull(state, version), context_msg
  end

  # Helper for push operations with better error handling
  defp assert_push_success(state, expected_version, transaction) do
    assert {:ok, new_state} = Pushing.push(state, expected_version, transaction, fn _ -> :ok end)
    new_state
  end

  defp create_basic_state do
    %State{
      cluster: Cluster,
      director: nil,
      epoch: nil,
      otp_name: :test,
      id: "test",
      foreman: self(),
      path: "/tmp/test",
      segment_recycler: nil,
      active_segment: nil,
      segments: [],
      last_version: Version.zero(),
      oldest_version: Version.zero(),
      waiting_pullers: %{},
      # Changed from :locked to :normal to avoid :not_ready errors
      mode: :normal,
      writer: nil,
      pending_pushes: %{},
      pending_transactions: %{},
      params: %{
        default_pull_limit: 100,
        max_pull_limit: 1000
      }
    }
  end

  defp create_mock_segment_with_versions(versions) do
    min_version = List.first(versions) || Version.zero()

    # Create actual encoded transactions with the specified versions
    transactions =
      Enum.map(versions, fn version ->
        TransactionTestSupport.new_log_transaction(Version.to_integer(version), %{
          "key" => "value_#{Version.to_integer(version)}"
        })
      end)

    %Segment{
      path: "/tmp/mock_segment",
      min_version: min_version,
      transactions: transactions
    }
  end
end
