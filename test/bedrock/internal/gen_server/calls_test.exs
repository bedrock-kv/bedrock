defmodule Bedrock.Internal.GenServer.CallsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.GenServer.Calls

  # A GenServer that dies mid-call with whatever reason the test asks for,
  # simulating a callee that crashes on a resource fault (e.g. the Shale
  # SegmentRecycler exiting with :enospc/:eacces instead of :shutdown).
  defmodule CrashingServer do
    @moduledoc false
    use GenServer

    def init(:ok), do: {:ok, :ok}

    def handle_call({:die, reason}, _from, state), do: {:stop, reason, state}
  end

  setup do
    # Unlinked on purpose: a linked start would deliver the crash to this
    # test process as its own EXIT signal, which is not what Calls.call/3
    # is meant to shield callers from.
    {:ok, pid} = GenServer.start(CrashingServer, :ok)
    %{pid: pid}
  end

  describe "call/3 against a callee that dies mid-call" do
    test "a posix exit reason is normalized to :unavailable", %{pid: pid} do
      assert Calls.call(pid, {:die, :eacces}, 1000) == {:error, :unavailable}
    end

    test "a different posix exit reason is normalized to :unavailable", %{pid: pid} do
      assert Calls.call(pid, {:die, :enospc}, 1000) == {:error, :unavailable}
    end

    test "{:shutdown, term} is normalized to :unavailable", %{pid: pid} do
      assert Calls.call(pid, {:die, {:shutdown, :disk_full}}, 1000) == {:error, :unavailable}
    end

    test "an exception-shaped crash reason is normalized to :unavailable", %{pid: pid} do
      reason = {%RuntimeError{message: "boom"}, [{__MODULE__, :handle_call, 3, []}]}
      assert Calls.call(pid, {:die, reason}, 1000) == {:error, :unavailable}
    end

    test "an arbitrary exit reason is normalized to :unavailable", %{pid: pid} do
      assert Calls.call(pid, {:die, :some_arbitrary_reason}, 1000) == {:error, :unavailable}
    end
  end
end
