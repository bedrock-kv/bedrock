defmodule Bedrock.Internal.RepoRetryBackoffTest do
  @moduledoc """
  Retry backoff must DECORRELATE contenders, not just space them out.

  The schedule was `(1 <<< retry_count) + rand(3)`: a ceiling that
  doubles, and jitter of one to three milliseconds. With N transactions
  contending on one key they all fail at nearly the same instant, all
  compute nearly the same delay, and all wake together — so they collide
  again, and the round trip repeats at twice the delay. The work is
  trivial; the wall clock is set by whichever contender happens to climb
  furthest up the ladder. Identical 100-way workloads measured 278ms,
  540ms, 1052ms and 2059ms on the same build.

  FDB's client answers this with FULL JITTER: the delay is a uniform draw
  from the whole interval below the ceiling, and only the CEILING grows
  (`NativeAPI.actor.cpp:4436`, `returnedBackoff *= random01()`, then
  `backoff = min(backoff * BACKOFF_GROWTH_RATE, maxBackoff)` at :4446;
  knobs at `ClientKnobs.cpp:67-69` — 10ms initial, 1s max, growth 2.0).

  Spreading contenders across `[1, ceiling]` means they no longer wake in
  lockstep, so each round some contender finds the key uncontended
  instead of every contender re-colliding.
  """
  use ExUnit.Case, async: true

  import Bitwise

  alias Bedrock.Internal.Repo

  describe "retry_delay_in_ms/1" do
    test "the ceiling doubles, exactly as before" do
      # The growth schedule is not what was wrong, so it does not change:
      # 1, 2, 4, ... FDB grows its ceiling the same way.
      for n <- 0..9 do
        assert Enum.max(for(_ <- 1..200, do: Repo.retry_delay_in_ms(n))) <= 1 <<< n
      end
    end

    test "the ceiling is capped, so a long retry chain cannot sleep unboundedly" do
      for n <- 10..40 do
        assert Repo.retry_delay_in_ms(n) <= 1000
      end
    end

    test "every delay is at least 1ms, so a retry never becomes a spin" do
      for n <- 0..12, _ <- 1..50 do
        assert Repo.retry_delay_in_ms(n) >= 1
      end
    end

    test "delays SPREAD across the interval rather than clustering at the ceiling" do
      # The property that actually fixes the pileup. Under the old
      # schedule every contender at the same retry count drew from a
      # 3ms-wide band at the top of the interval; here they are spread
      # across the whole thing, so they stop waking together.
      samples = for _ <- 1..2000, do: Repo.retry_delay_in_ms(9)
      ceiling = 1 <<< 9

      assert Enum.min(samples) < div(ceiling, 4),
             "no contender drew from the bottom quarter — delays are still clustered high"

      assert Enum.max(samples) > div(3 * ceiling, 4),
             "no contender drew from the top quarter — the ceiling is not being used"

      # A wide spread is the point: distinct values, not a narrow band.
      assert samples |> Enum.uniq() |> length() > 100
    end

    test "two contenders at the same retry count usually draw DIFFERENT delays" do
      # The lockstep failure, stated directly: under `(1 <<< n) + rand(3)`
      # two contenders collided on the same millisecond about a third of
      # the time. Full jitter over a 512ms interval makes that rare.
      collisions =
        Enum.count(1..1000, fn _ ->
          Repo.retry_delay_in_ms(9) == Repo.retry_delay_in_ms(9)
        end)

      assert collisions < 50, "contenders still wake in lockstep (#{collisions}/1000 collided)"
    end
  end
end
