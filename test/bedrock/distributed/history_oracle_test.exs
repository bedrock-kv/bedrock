defmodule Bedrock.Distributed.HistoryOracleTest do
  use ExUnit.Case, async: true

  alias Bedrock.Test.History.Driver
  alias Bedrock.Test.History.Gates
  alias Bedrock.Test.History.Oracle

  defp tx(id, start, stop, status, ops, reads \\ []),
    do: %{id: id, invoke: start, complete: stop, status: status, ops: ops, reads: reads}

  test "overlapping transactions may serialize in a different completion order" do
    history = [tx(1, 0, 3, :committed, [{:put, "k", "one"}]), tx(2, 1, 2, :committed, [{:put, "k", "two"}])]
    assert {:ok, _} = Oracle.check(%{}, history, %{"k" => "two"})
    assert {:ok, _} = Oracle.check(%{}, history, %{"k" => "one"})
  end

  test "real-time precedence forbids reordering nonoverlapping commits" do
    history = [tx(1, 0, 1, :committed, [{:put, "k", "one"}]), tx(2, 2, 3, :committed, [{:put, "k", "two"}])]
    assert {:error, :no_serialization} = Oracle.check(%{}, history, %{"k" => "one"})
    assert {:ok, [1, 2]} = Oracle.check(%{}, history, %{"k" => "two"})
  end

  test "acknowledged commits cannot disappear or execute twice" do
    history = [tx(1, 0, 1, :committed, [{:add, "k", 1}])]
    assert {:error, :no_serialization} = Oracle.check(%{}, history, %{})
    assert {:error, :no_serialization} = Oracle.check(%{}, history, %{"k" => <<2::64-little>>})
  end

  test "unknown attempts may apply or omit while acknowledged retries remain mandatory" do
    history = [tx(1, 0, 1, :unknown, [{:add, "k", 1}]), tx(2, 2, 3, :committed, [{:add, "k", 1}])]
    assert {:ok, _} = Oracle.check(%{}, history, %{"k" => <<1::64-little>>})
    assert {:ok, _} = Oracle.check(%{}, history, %{"k" => <<2::64-little>>})
    assert {:error, :no_serialization} = Oracle.check(%{}, history, %{})
  end

  test "an included unknown attempt must obey its observed reads" do
    history = [tx(1, 0, 1, :unknown, [{:get, "k"}, {:put, "x", "yes"}], [{:get, "k", "missing"}])]
    assert {:error, :no_serialization} = Oracle.check(%{}, history, %{"x" => "yes"})
    assert {:ok, []} = Oracle.check(%{}, history, %{})
  end

  test "aborted transactions never contribute writes" do
    assert {:error, :no_serialization} = Oracle.check(%{}, [tx(1, 0, 1, :aborted, [{:put, "k", "v"}])], %{"k" => "v"})
  end

  test "two overlapping reservations cannot both observe the same absence" do
    range = {"r/", "r0"}

    history =
      for {id, key} <- [{1, "r/a"}, {2, "r/b"}],
          do: tx(id, 0, 2, :committed, [{:reserve, range, key}], [{:reserve, true}])

    assert {:error, :no_serialization} = Oracle.check(%{}, history, %{"r/a" => "reserved", "r/b" => "reserved"})
  end

  test "interpreter respects ordered sets, half-open clears, arithmetic and transfers" do
    ops = [
      {:put, "a", "old"},
      {:put, "b", "endpoint"},
      {:clear_range, "a", "b"},
      {:put, "a", "new"},
      {:get, "a"},
      {:range, "a", "c"},
      {:add, "alice", 10},
      {:transfer, "alice", "bob", 3}
    ]

    {map, reads} = Oracle.evaluate(%{}, ops)
    assert map == %{"a" => "new", "b" => "endpoint", "alice" => <<7::64-little>>, "bob" => <<3::64-little>>}

    assert reads == [
             {:get, "a", "new"},
             {:range, [{"a", "new"}, {"b", "endpoint"}]},
             {:transfer, <<10::64-little>>, nil, true}
           ]
  end

  test "a timed-out attempt may commit after a later acknowledged operation" do
    history = [tx(1, 0, 1, :unknown, [{:put, "k", "late"}]), tx(2, 2, 3, :committed, [{:put, "k", "ack"}])]
    assert {:ok, [2, 1]} = Oracle.check(%{}, history, %{"k" => "late"})
  end

  test "transfer observations retain balances even if later writes hide the bad read" do
    initial = %{"a" => <<10::64-little>>, "b" => <<0::64-little>>}
    bad = tx(1, 0, 1, :committed, [{:transfer, "a", "b", 1}], [{:transfer, <<99::64-little>>, <<0::64-little>>, true}])
    overwrite = tx(2, 2, 3, :committed, [{:put, "a", <<0::64-little>>}, {:put, "b", <<0::64-little>>}])

    assert {:error, :no_serialization} =
             Oracle.check(initial, [bad, overwrite], %{"a" => <<0::64-little>>, "b" => <<0::64-little>>})
  end

  test "driver distinguishes acknowledgments from rollback returns and ambiguous failures" do
    observations = [{:get, "k", nil}]
    assert Driver.classify_return(observations, observations) == :committed
    assert Driver.classify_return({:error, :key_out_of_range}, observations) == :aborted
    assert Driver.classify_return(:unexpected, observations) == :unknown

    assert Driver.classify_exception(
             %RuntimeError{message: "Transaction retry limit exceeded after 0 attempts. Last error: :aborted"},
             observations
           ) == :aborted

    assert Driver.classify_exception(%RuntimeError{message: "unrelated failure. Last error: :aborted"}, observations) ==
             :unknown

    assert Driver.classify_exception(%RuntimeError{message: "timeout"}, observations) == :unknown
    assert Driver.classify_exception(%RuntimeError{message: "read timeout before callback completes"}, nil) == :aborted
  end

  test "disarming a consumed gate releases its blocked participant" do
    gates = start_supervised!({Agent, fn -> nil end})
    Gates.arm(gates, %{stage: :test, match: fn _ -> true end, owner: self()})
    task = Task.async(fn -> Gates.pause(gates, :test, "key") end)
    assert_receive {:history_gate, :test, _, _, "key"}
    assert :ok = Gates.disarm(gates)
    assert :ok = Task.await(task, 1_000)
  end
end
