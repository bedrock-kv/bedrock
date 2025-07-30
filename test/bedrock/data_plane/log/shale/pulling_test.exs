defmodule Bedrock.DataPlane.Log.Shale.PullingTest do
  use ExUnit.Case, async: true
  alias Bedrock.DataPlane.Log.EncodedTransaction
  alias Bedrock.DataPlane.Log.Shale.{Pulling, Segment, State}

  @default_params %{default_pull_limit: 1000, max_pull_limit: 2000}

  describe "pull/3" do
    setup do
      transactions =
        [
          {0, %{}},
          {1, %{"a" => "1"}},
          {2, %{"b" => "2"}},
          {3, %{"c" => "3"}}
        ]
        |> Enum.map(&EncodedTransaction.encode/1)
        |> Enum.reverse()

      segment = %Segment{
        min_version: 0,
        transactions: transactions
      }

      state = %State{
        active_segment: segment,
        segments: [],
        oldest_version: 0,
        last_version: 3,
        mode: :ready,
        params: @default_params
      }

      {:ok, %{state: state, segment: segment}}
    end

    test "returns waiting_for when from_version >= last_version", %{state: state} do
      assert {:waiting_for, 3} = Pulling.pull(state, 3)
      assert {:waiting_for, 4} = Pulling.pull(state, 4)
    end

    test "returns error when from_version is too old", %{state: state} do
      assert {:error, :version_too_old} = Pulling.pull(state, -1)
    end

    test "returns transactions within version range", %{state: state} do
      {:ok, _state, transactions} = Pulling.pull(state, 1)
      assert length(transactions) == 2
      assert Enum.map(transactions, &EncodedTransaction.version(&1)) == [2, 3]
    end

    test "respects last_version parameter", %{state: state} do
      {:ok, _state, transactions} = Pulling.pull(state, 1, last_version: 2)
      assert length(transactions) == 1
      assert EncodedTransaction.version(hd(transactions)) == 2
    end

    test "handles recovery mode correctly", %{state: state} do
      locked_state = %{state | mode: :locked}
      assert {:error, :not_ready} = Pulling.pull(locked_state, 1)
      assert {:ok, _, _} = Pulling.pull(locked_state, 1, recovery: true)
    end

    test "respects pull limits", %{state: state} do
      {:ok, _state, transactions} = Pulling.pull(state, 1, limit: 1)
      assert length(transactions) == 1
    end

    test "filters by key range", %{state: state} do
      {:ok, _state, transactions} = Pulling.pull(state, 0, key_range: {"a", "c"})

      assert transactions
             |> Enum.map(&EncodedTransaction.decode!/1)
             |> Enum.map(&elem(&1, 1))
             |> Enum.flat_map(&Map.keys/1)
             |> Enum.sort() ==
               ["a", "b"]
    end

    test "can exclude values", %{state: state} do
      {:ok, _state, transactions} = Pulling.pull(state, 1, exclude_values: true)

      assert transactions
             |> Enum.map(&EncodedTransaction.decode!/1)
             |> Enum.map(&elem(&1, 1))
             |> Enum.map(&Map.values/1)
             |> Enum.all?(&(&1 == [<<>>]))
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
      assert Pulling.determine_pull_limit(nil, state) == 1000
      assert Pulling.determine_pull_limit(500, state) == 500
      assert Pulling.determine_pull_limit(3000, state) == 2000
    end
  end
end
