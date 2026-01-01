defmodule Bedrock.Internal.GenServer.RepliesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.Internal.GenServer.Replies

  describe "reply/2" do
    test "creates simple reply tuple" do
      state = %{count: 1}
      result = :ok
      assert Replies.reply(state, result) == {:reply, :ok, %{count: 1}}
    end

    test "preserves state and result" do
      state = "my_state"
      result = {:ok, 42}
      assert Replies.reply(state, result) == {:reply, {:ok, 42}, "my_state"}
    end

    property "always produces 3-tuple with :reply tag" do
      check all(
              state <- term(),
              result <- term()
            ) do
        {tag, res, st} = Replies.reply(state, result)
        assert tag == :reply
        assert res == result
        assert st == state
      end
    end
  end

  describe "reply/3 with continue" do
    test "creates reply tuple with continue" do
      state = %{data: "test"}
      result = :ok

      assert Replies.reply(state, result, continue: :process_next) ==
               {:reply, :ok, %{data: "test"}, {:continue, :process_next}}
    end

    test "handles atom continue action" do
      assert Replies.reply(:state, :result, continue: :my_action) ==
               {:reply, :result, :state, {:continue, :my_action}}
    end

    test "handles tuple continue action" do
      assert Replies.reply(:state, :result, continue: {:action, :data}) ==
               {:reply, :result, :state, {:continue, {:action, :data}}}
    end

    property "always produces 4-tuple with continue" do
      check all(
              state <- term(),
              result <- term(),
              action <- term()
            ) do
        {tag, res, st, continue} = Replies.reply(state, result, continue: action)
        assert tag == :reply
        assert res == result
        assert st == state
        assert continue == {:continue, action}
      end
    end
  end

  describe "noreply/1" do
    test "creates simple noreply tuple" do
      state = %{active: true}
      assert Replies.noreply(state) == {:noreply, %{active: true}}
    end

    test "works with empty options list" do
      assert Replies.noreply(:state, []) == {:noreply, :state}
    end

    property "always produces 2-tuple with :noreply tag" do
      check all(state <- term()) do
        {tag, st} = Replies.noreply(state)
        assert tag == :noreply
        assert st == state
      end
    end
  end

  describe "noreply/2 with continue" do
    test "creates noreply tuple with continue" do
      state = :my_state

      assert Replies.noreply(state, continue: :handle_timeout) ==
               {:noreply, :my_state, {:continue, :handle_timeout}}
    end

    test "handles complex continue actions" do
      assert Replies.noreply(:state, continue: {:complex, :action, :data}) ==
               {:noreply, :state, {:continue, {:complex, :action, :data}}}
    end

    property "always produces 3-tuple with continue" do
      check all(
              state <- term(),
              action <- term()
            ) do
        {tag, st, continue} = Replies.noreply(state, continue: action)
        assert tag == :noreply
        assert st == state
        assert continue == {:continue, action}
      end
    end
  end

  describe "noreply/2 with timeout" do
    test "creates noreply tuple with timeout" do
      state = %{waiting: true}

      assert Replies.noreply(state, timeout: 5000) ==
               {:noreply, %{waiting: true}, 5000}
    end

    test "handles zero timeout" do
      assert Replies.noreply(:state, timeout: 0) == {:noreply, :state, 0}
    end

    test "handles :infinity timeout" do
      assert Replies.noreply(:state, timeout: :infinity) == {:noreply, :state, :infinity}
    end

    property "always produces 3-tuple with timeout" do
      check all(
              state <- term(),
              timeout <- one_of([integer(0..100_000), constant(:infinity)])
            ) do
        {tag, st, t} = Replies.noreply(state, timeout: timeout)
        assert tag == :noreply
        assert st == state
        assert t == timeout
      end
    end
  end

  describe "noreply/2 error handling" do
    test "raises on invalid options" do
      assert_raise RuntimeError, ~r/Invalid options/, fn ->
        Replies.noreply(:state, invalid: :option)
      end
    end

    test "raises on multiple options" do
      assert_raise RuntimeError, ~r/Invalid options/, fn ->
        Replies.noreply(:state, continue: :action, timeout: 5000)
      end
    end

    test "raises on unknown keyword" do
      assert_raise RuntimeError, ~r/Invalid options/, fn ->
        Replies.noreply(:state, unknown: :value)
      end
    end
  end

  describe "stop/2" do
    test "creates stop tuple" do
      state = %{stopped: true}
      reason = :normal
      assert Replies.stop(state, reason) == {:stop, :normal, %{stopped: true}}
    end

    test "handles shutdown reason" do
      assert Replies.stop(:state, :shutdown) == {:stop, :shutdown, :state}
    end

    test "handles error reasons" do
      assert Replies.stop(:state, {:error, :timeout}) ==
               {:stop, {:error, :timeout}, :state}
    end

    property "always produces 3-tuple with :stop tag" do
      check all(
              state <- term(),
              reason <- term()
            ) do
        {tag, r, st} = Replies.stop(state, reason)
        assert tag == :stop
        assert r == reason
        assert st == state
      end
    end
  end

  describe "integration patterns" do
    test "can chain operations in GenServer callbacks" do
      # Simulating common GenServer patterns
      state = %{count: 0}

      # handle_call returning reply
      reply_tuple = Replies.reply(state, :ok)
      assert match?({:reply, :ok, %{count: 0}}, reply_tuple)

      # handle_call with continue
      reply_continue = Replies.reply(state, {:ok, 1}, continue: :increment)
      assert match?({:reply, {:ok, 1}, %{count: 0}, {:continue, :increment}}, reply_continue)

      # handle_cast
      noreply_tuple = Replies.noreply(%{state | count: 1})
      assert match?({:noreply, %{count: 1}}, noreply_tuple)

      # handle_info with timeout
      noreply_timeout = Replies.noreply(state, timeout: 5000)
      assert match?({:noreply, %{count: 0}, 5000}, noreply_timeout)

      # terminate
      stop_tuple = Replies.stop(state, :normal)
      assert match?({:stop, :normal, %{count: 0}}, stop_tuple)
    end
  end
end
