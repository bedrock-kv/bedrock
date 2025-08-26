defmodule Bedrock.DataPlane.Log.Shale.WalSegmentIssuesTest do
  use ExUnit.Case, async: false

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Log.Shale.Pulling
  alias Bedrock.DataPlane.Log.Shale.Pushing
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.TransactionTestSupport
  alias Bedrock.DataPlane.Version

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
          # Use current last_version as expected_version for immediate processing
          expected_version = current_state.last_version
          transaction = TransactionTestSupport.new_log_transaction(i, %{"data" => large_data})
          # Perform actual push operation to trigger real segment rotation logic
          case Pushing.push(current_state, expected_version, transaction, fn _ -> :ok end) do
            {:ok, new_state} ->
              segment_count = length([new_state.active_segment | new_state.segments])
              {new_state, [segment_count | counts]}

            {:wait, _new_state} ->
              # This shouldn't happen with expected_version == last_version
              flunk("Unexpected wait state when version should match")

            {:error, reason} ->
              flunk("Push failed: #{inspect(reason)}")
          end
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
      v1 = Version.from_integer(100)
      v2 = Version.from_integer(200)
      v3 = Version.from_integer(300)

      # Create a mock segment with these transactions
      state = create_test_state_with_versions([v1, v2, v3])

      # Test pulling from exact version match
      case Pulling.pull(state, v2) do
        {:ok, _state, transactions} ->
          # Should get transactions AFTER v2 (i.e., just v3)
          assert length(transactions) == 1, """
          Expected 1 transaction after version #{inspect(v2)}, got #{length(transactions)}.
          This indicates exact version boundary logic is wrong.
          Transactions: #{inspect(transactions)}
          """

        {:waiting_for, _version} ->
          flunk("Should not be waiting when transactions exist after the requested version")

        {:error, reason} ->
          flunk("Unexpected error: #{inspect(reason)}")
      end
    end

    test "transaction stream should return empty when requesting from last version" do
      v1 = Version.from_integer(100)
      v2 = Version.from_integer(200)
      v3 = Version.from_integer(300)

      state = create_test_state_with_versions([v1, v2, v3])

      # Pull from the last version - should trigger waiting behavior
      case Pulling.pull(state, v3) do
        {:waiting_for, ^v3} ->
          # This is correct - should wait for transactions after v3
          assert true

        {:ok, _state, transactions} ->
          flunk("""
          Expected waiting behavior when pulling from last version #{inspect(v3)},
          but got #{length(transactions)} transactions: #{inspect(transactions)}
          """)

        {:error, reason} ->
          flunk("Unexpected error: #{inspect(reason)}")
      end
    end

    test "from_segments handles large gaps in versions correctly" do
      # Simulate the issue we saw: large version gaps within single segment
      versions = [
        # Large starting version
        Version.from_integer(1_000_000),
        # Big gap
        Version.from_integer(2_000_000),
        # Even bigger gap
        Version.from_integer(5_000_000)
      ]

      state = create_test_state_with_versions(versions)
      segments = [state.active_segment | state.segments]

      # Test pulling from middle version
      middle_version = Version.from_integer(2_000_000)

      case TransactionStreams.from_segments(segments, middle_version) do
        {:ok, stream} ->
          transactions = Enum.take(stream, 10)

          assert length(transactions) > 0, """
          Expected transactions after version #{inspect(middle_version)} with large version gaps,
          but got empty stream. This indicates from_segments can't handle large version ranges.
          """

        {:error, reason} ->
          flunk("from_segments failed with large version gaps: #{inspect(reason)}")
      end
    end

    test "segment loading includes all necessary segments for version range" do
      # Create segments with actual transactions spanning different version ranges
      segment1_versions = Enum.map([1, 50, 100], &Version.from_integer/1)
      segment2_versions = Enum.map([101, 150, 200], &Version.from_integer/1)
      segment3_versions = Enum.map([201, 250, 300], &Version.from_integer/1)

      segments = [
        # min_version = 1
        create_mock_segment_with_versions(segment1_versions),
        # min_version = 101
        create_mock_segment_with_versions(segment2_versions),
        # min_version = 201
        create_mock_segment_with_versions(segment3_versions)
      ]

      state = %{create_basic_state() | active_segment: hd(segments), segments: tl(segments)}

      # Request transactions up to version 250 - should load first 3 segments
      target_version = Version.from_integer(250)

      case Pulling.ensure_necessary_segments_are_loaded(target_version, [state.active_segment | state.segments]) do
        {:ok, loaded_segments} ->
          assert length(loaded_segments) >= 2, """
          Expected multiple segments loaded for version range up to #{inspect(target_version)},
          but only got #{length(loaded_segments)} segments. This indicates segment loading
          is not including all necessary segments for the requested range.
          """

        {:error, reason} ->
          flunk("Segment loading failed: #{inspect(reason)}")
      end
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
        version_int = Version.to_integer(version)
        TransactionTestSupport.new_log_transaction(version_int, %{"key" => "value_#{version_int}"})
      end)

    %Segment{
      path: "/tmp/mock_segment",
      min_version: min_version,
      transactions: transactions
    }
  end
end
