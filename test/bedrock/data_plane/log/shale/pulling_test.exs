defmodule Bedrock.DataPlane.Log.Shale.PullingTest do
  use ExUnit.Case, async: true
  alias Bedrock.DataPlane.Log.Shale.Pulling
  alias Bedrock.DataPlane.Log.Shale.State

  setup do
    # Setup initial t for tests
    t = %State{
      log: :ets.new(:log, [:protected, :ordered_set]),
      mode: :running,
      last_version: 5,
      oldest_version: 1,
      params: %{default_pull_limit: 5, max_pull_limit: 10}
    }

    # Insert some dummy transactions
    :ets.insert(t.log, {1, %{a: :b}})
    :ets.insert(t.log, {2, %{c: 21}})
    :ets.insert(t.log, {3, %{j: 16}})
    :ets.insert(t.log, {4, %{c: 32}})
    :ets.insert(t.log, {5, %{m: "asd"}})

    {:ok, t: t}
  end

  test "pull returns transactions within the specified range", %{t: t} do
    assert {:ok, ^t, transactions} = Pulling.pull(t, 1, limit: 3, last_version: 3)

    assert [
             {2, %{c: 21}},
             {3, %{j: 16}}
           ] = transactions
  end

  test "pull returns transactions within the specified range, up to it's limit", %{t: t} do
    assert {:ok, ^t, transactions} = Pulling.pull(t, 1, limit: 1, last_version: 3)

    assert [
             {2, %{c: 21}}
           ] = transactions
  end

  test "pull returns no transactions when the last_version is the same as the from_version",
       %{
         t: t
       } do
    assert {:ok, ^t, transactions} = Pulling.pull(t, 1, last_version: 1)

    assert [] = transactions
  end

  test "pull returns transactions within the specified range, up to it's limit, from the start",
       %{t: t} do
    assert {:ok, ^t, transactions} = Pulling.pull(t, :start, limit: 1, last_version: 3)

    assert [
             {1, %{a: :b}}
           ] = transactions
  end

  test "pull returns all transactions when pulling from the start",
       %{t: t} do
    assert {:ok, ^t, transactions} = Pulling.pull(t, :start)

    assert [
             {1, %{a: :b}},
             {2, %{c: 21}},
             {3, %{j: 16}},
             {4, %{c: 32}},
             {5, %{m: "asd"}}
           ] = transactions
  end

  test "pull returns transactions wirt",
       %{t: t} do
    assert {:ok, ^t, transactions} = Pulling.pull(t, :start, limit: 1, last_version: 3)

    assert [
             {1, %{a: :b}}
           ] = transactions
  end

  test "pull returns error for invalid last_version", %{t: t} do
    assert {:error, :invalid_last_version} = Pulling.pull(t, 1, limit: 3, last_version: 0)
  end

  test "pull returns error when locked and not in recovery", %{t: t} do
    t = %{t | mode: :locked}
    assert {:error, :not_ready} = Pulling.pull(t, 1, limit: 3, last_version: 3)
  end

  test "pull returns transactions when locked and in recovery", %{t: t} do
    t = %{t | mode: :locked}

    assert {:ok, ^t, _transactions} = Pulling.pull(t, 1, recovery: true)
  end

  test "pull returns waiting_for when from_version is greater than last_version", %{t: t} do
    assert {:waiting_for, 11} = Pulling.pull(t, 11, limit: 3, last_version: 12)
  end

  test "pull returns transactions when from_version is :start", %{t: t} do
    assert {:ok, ^t, transactions} = Pulling.pull(t, :start, limit: 3, last_version: 3)
    assert length(transactions) == 3
  end
end
