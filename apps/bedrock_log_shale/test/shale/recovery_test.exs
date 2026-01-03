defmodule Bedrock.DataPlane.Log.Shale.RecoveryTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Recovery
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @moduletag :tmp_dir

  # Helper functions for common test patterns
  defp version(n), do: Version.from_integer(n)

  setup %{tmp_dir: tmp_dir} do
    {:ok, recycler} =
      start_supervised({SegmentRecycler, path: tmp_dir, min_available: 1, max_available: 1, segment_size: 1024 * 1024})

    state = %State{
      mode: :locked,
      path: tmp_dir,
      segment_recycler: recycler,
      active_segment: nil,
      segments: [],
      writer: nil,
      oldest_version: version(0),
      last_version: version(0)
    }

    {:ok, state: state, tmp_dir: tmp_dir}
  end

  describe "recover_from/4" do
    test "returns error when not in locked mode", %{state: state} do
      unlocked_state = %{state | mode: :running}

      assert {:error, :lock_required} =
               Recovery.recover_from(
                 unlocked_state,
                 :source,
                 version(1),
                 version(2)
               )
    end

    test "successfully recovers with no transactions", %{state: state} do
      source_log = setup_mock_log([])
      expected_version = version(1)

      assert {:ok, %{mode: :running, oldest_version: ^expected_version, last_version: ^expected_version}} =
               Recovery.recover_from(
                 state,
                 source_log,
                 expected_version,
                 expected_version
               )
    end

    test "correctly handles recovery when first_version equals last_version", %{state: state} do
      # This test verifies the fix for the issue where logs would report having
      # version ranges but had no segments loaded, causing :not_found errors
      v = version(5)

      assert {:ok, %{mode: :running, oldest_version: ^v, last_version: ^v, active_segment: segment, writer: writer}} =
               Recovery.recover_from(
                 state,
                 nil,
                 v,
                 v
               )

      assert segment
      assert writer
    end

    test "successfully recovers with valid transactions", %{state: state} do
      first_version = version(1)
      last_version = version(2)

      transactions = [
        create_encoded_tx(first_version, %{"data" => "test1"}),
        create_encoded_tx(last_version, %{"data" => "test2"})
      ]

      source_log = setup_mock_log(transactions)

      assert {:ok, %{mode: :running, oldest_version: ^first_version, last_version: ^last_version}} =
               Recovery.recover_from(
                 state,
                 source_log,
                 first_version,
                 last_version
               )
    end

    test "handles unavailable source log", %{state: state} do
      source_log = setup_failing_mock_log(:unavailable)

      assert {:error, {:source_log_unavailable, ^source_log}} =
               Recovery.recover_from(
                 state,
                 source_log,
                 version(1),
                 version(2)
               )
    end
  end

  describe "pull_transactions/4" do
    test "sets versions correctly when first_version equals last_version", %{state: state} do
      # This test covers both empty transaction list and version consistency scenarios
      v = version(10)
      source_log = setup_mock_log([])

      assert {:ok, %{oldest_version: ^v, last_version: ^v}} =
               Recovery.pull_transactions(
                 state,
                 source_log,
                 v,
                 v
               )
    end

    test "handles invalid transaction data", %{state: state} do
      source_log = setup_mock_log(["invalid"])

      assert {:error, :invalid_transaction} =
               Recovery.pull_transactions(
                 state,
                 source_log,
                 version(1),
                 version(2)
               )
    end
  end

  defp create_encoded_tx(version, data) do
    mutations = Enum.map(data, fn {key, value} -> {:set, key, value} end)

    transaction = %{
      mutations: mutations,
      read_conflicts: [],
      write_conflicts: [],
      read_version: nil
    }

    encoded = Transaction.encode(transaction)
    {:ok, encoded_with_id} = Transaction.add_commit_version(encoded, version)
    encoded_with_id
  end

  defp setup_mock_log(transactions) do
    spawn_link(fn ->
      receive do
        {:"$gen_call", {from, ref}, {:pull, _version, _opts}} ->
          send(from, {ref, {:ok, transactions}})
      after
        500 -> :timeout
      end
    end)
  end

  defp setup_failing_mock_log(error) do
    spawn_link(fn ->
      receive do
        {:"$gen_call", {from, ref}, {:pull, _version, _opts}} ->
          send(from, {ref, {:error, error}})
      after
        500 -> :timeout
      end
    end)
  end
end
