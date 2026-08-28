defmodule Bedrock.DataPlane.CommitProxy.AdaptiveBatchingTest do
  @moduledoc """
  A batch should wait only when waiting would collect something.

  The proxy closed its batch on a zero timeout, which fires as soon as the
  mailbox is empty. With request/response clients the mailbox is almost
  always empty, so the window closed on the first transaction and
  `max_latency_in_ms` was never reached: measured batch sizes were 1.0 at
  one-way concurrency and still only 1.41 at 128-way. Every transaction
  paid a full finalization round (~3.8ms of resolver plus log push).

  Holding the window fixes throughput and hurts idle latency. Measured,
  uncontended writes to distinct keys:

      policy        conc=1     conc=32     conc=128    p50 @128
      close now     1768/s      6749/s      7586/s     15-17ms
      hold 1ms       490/s     15598/s     24137/s      5-6ms
      adaptive      1952/s     17044/s     25962/s      4-6ms

  So the wait is decided by whether recent batches actually FILLED. An
  idle proxy sees batches of one, keeps its average low, and waits zero —
  a lone transaction is never delayed. Under load the average climbs
  within a few batches and the proxy starts amortizing.

  FDB adapts the same knob from the other side: its interval tracks a
  fraction of observed latency, smoothed, clamped to
  `[COMMIT_TRANSACTION_BATCH_INTERVAL_MIN, ..._MAX]` = `[1ms, 20ms]`
  (`CommitProxyServer.actor.cpp:2843-2849`, knobs at
  `ServerKnobs.cpp:701-704`). Its floor is 1ms, so an idle FDB commit
  always pays it; ours can reach zero because version advancement is
  already guaranteed separately by the empty-transaction timeout.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batching

  describe "observe_batch/2" do
    test "a run of single-transaction batches keeps the average at one" do
      average = Enum.reduce(1..20, 1.0, fn _, avg -> Batching.observe_batch(avg, 1) end)
      assert_in_delta average, 1.0, 0.001
    end

    test "a run of full batches raises the average" do
      average = Enum.reduce(1..20, 1.0, fn _, avg -> Batching.observe_batch(avg, 10) end)
      assert average > 9.0
    end

    test "it is a moving average, so a single busy batch does not latch it high" do
      # One full batch among singles must not commit the proxy to waiting
      # forever; the average has to decay back.
      busy = Batching.observe_batch(1.0, 10)
      assert busy > 1.5

      decayed = Enum.reduce(1..20, busy, fn _, avg -> Batching.observe_batch(avg, 1) end)
      assert_in_delta decayed, 1.0, 0.01
    end
  end

  describe "hold_in_ms/1" do
    test "an idle proxy waits ZERO — a lone transaction is never delayed" do
      # The property the old zero-timeout got right, and which any
      # batching policy must keep: nothing to batch with, so no wait.
      assert Batching.hold_in_ms(1.0) == 0
    end

    test "a filling proxy waits, so the finalization round is amortized" do
      assert Batching.hold_in_ms(8.0) > 0
    end

    test "the wait is small — 1ms, FDB's floor, not a latency budget" do
      # The sweep showed larger holds are strictly worse: 8ms cost 12x at
      # idle and LOST throughput at 128-way versus 1ms.
      assert Batching.hold_in_ms(100.0) == 1
    end

    test "the switch sits above one, so noise in the average cannot trip it" do
      assert Batching.hold_in_ms(1.4) == 0
      assert Batching.hold_in_ms(1.6) == 1
    end
  end

  describe "the two composed: a proxy learns and unlearns" do
    test "idle proxy stays at zero wait through a long quiet run" do
      average = Enum.reduce(1..50, 1.0, fn _, avg -> Batching.observe_batch(avg, 1) end)
      assert Batching.hold_in_ms(average) == 0
    end

    test "a proxy under load starts waiting within a few batches" do
      average = Enum.reduce(1..3, 1.0, fn _, avg -> Batching.observe_batch(avg, 10) end)
      assert Batching.hold_in_ms(average) == 1
    end

    test "and stops waiting again once the load goes away" do
      loaded = Enum.reduce(1..10, 1.0, fn _, avg -> Batching.observe_batch(avg, 10) end)
      assert Batching.hold_in_ms(loaded) == 1

      quiet = Enum.reduce(1..10, loaded, fn _, avg -> Batching.observe_batch(avg, 1) end)
      assert Batching.hold_in_ms(quiet) == 0
    end
  end
end
