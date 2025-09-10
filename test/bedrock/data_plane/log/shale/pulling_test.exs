defmodule Bedrock.DataPlane.Log.Shale.PullingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Pulling
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @default_params %{default_pull_limit: 1000, max_pull_limit: 2000}

  describe "pull/3" do
    setup do
      transactions =
        [
          {Version.from_integer(0), %{}},
          {Version.from_integer(1), %{"a" => "1"}},
          {Version.from_integer(2), %{"b" => "2"}},
          {Version.from_integer(3), %{"c" => "3"}}
        ]
        |> Enum.map(fn {version, writes} ->
          TransactionTestSupport.new_log_transaction(version, writes)
        end)
        |> Enum.reverse()

      segment = %Segment{
        min_version: Version.from_integer(0),
        transactions: transactions
      }

      state = %State{
        active_segment: segment,
        segments: [],
        oldest_version: Version.from_integer(0),
        last_version: Version.from_integer(3),
        mode: :ready,
        params: @default_params
      }

      {:ok, %{state: state, segment: segment}}
    end

    test "returns waiting_for when from_version >= last_version", %{state: state} do
      version_3 = Version.from_integer(3)
      version_4 = Version.from_integer(4)
      # Request for version 3 when last_version is 3 should wait (nothing after version 3)
      assert {:waiting_for, ^version_3} = Pulling.pull(state, version_3)
      # Request for version 4 when last_version is 3 should wait (need version 4+)
      assert {:waiting_for, ^version_4} = Pulling.pull(state, version_4)
    end

    test "returns error when from_version is too old", %{state: state} do
      # Test with a version that's older than oldest_version in the state
      # The state has oldest_version = 0, so we'll create a state with older version = 1 to test this properly
      older_state = %{state | oldest_version: Version.from_integer(1)}
      assert {:error, :version_too_old} = Pulling.pull(older_state, Version.from_integer(0))
    end

    test "returns transactions within version range", %{state: state} do
      assert {:ok, _state, transactions} = Pulling.pull(state, Version.from_integer(1))
      assert length(transactions) == 2

      versions = Enum.map(transactions, &TransactionTestSupport.extract_log_version/1)
      assert versions == [Version.from_integer(2), Version.from_integer(3)]
    end

    test "respects last_version parameter", %{state: state} do
      assert {:ok, _state, transactions} =
               Pulling.pull(state, Version.from_integer(1), last_version: Version.from_integer(2))

      versions = Enum.map(transactions, &TransactionTestSupport.extract_log_version/1)
      assert versions == [Version.from_integer(2)]
    end

    test "handles recovery mode correctly", %{state: state} do
      locked_state = %{state | mode: :locked}
      version_1 = Version.from_integer(1)

      assert {:error, :not_ready} = Pulling.pull(locked_state, version_1)
      assert {:ok, _, _} = Pulling.pull(locked_state, version_1, recovery: true)
    end

    test "respects pull limits", %{state: state} do
      assert {:ok, _state, transactions} = Pulling.pull(state, Version.from_integer(1), limit: 1)
      assert length(transactions) == 1
    end

    test "boundary conditions: pull at or beyond last_version waits correctly", %{state: state} do
      # When requesting at or beyond last_version, should wait for new transactions
      version_3 = Version.from_integer(3)
      version_4 = Version.from_integer(4)
      version_5 = Version.from_integer(5)

      # state.last_version is 3, so requesting at/beyond should wait
      assert {:waiting_for, ^version_3} = Pulling.pull(state, version_3)
      assert {:waiting_for, ^version_4} = Pulling.pull(state, version_4)
      assert {:waiting_for, ^version_5} = Pulling.pull(state, version_5)
    end
  end

  describe "check_last_version/2" do
    test "validates last_version correctly" do
      assert {:ok, nil} = Pulling.check_last_version(nil, 1)
      assert {:ok, 2} = Pulling.check_last_version(2, 1)
      assert {:error, :invalid_last_version} = Pulling.check_last_version(1, 2)
    end
  end

  describe "determine_pull_limit/2" do
    test "respects default and max limits" do
      state = %State{params: @default_params}
      assert 1000 = Pulling.determine_pull_limit(nil, state)
      assert 500 = Pulling.determine_pull_limit(500, state)
      assert 2000 = Pulling.determine_pull_limit(3000, state)
    end
  end
end
