defmodule Bedrock.Internal.Time.IntervalTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Time.Interval

  describe "from/2" do
    test "creates interval from valid unit and non-negative integer" do
      assert Interval.from(:second, 10) == {:second, 10}
    end

    test "raises error for invalid unit" do
      assert_raise FunctionClauseError, fn -> Interval.from(:invalid, 10) end
    end

    test "raises error for negative integer" do
      assert_raise FunctionClauseError, fn -> Interval.from(:second, -1) end
    end
  end

  describe "between/3" do
    setup do
      start = ~U[2023-01-01T00:00:00Z]
      stop = ~U[2023-01-01T00:00:10Z]
      %{start: start, stop: stop}
    end

    test "calculates interval between two DateTime values", %{start: start, stop: stop} do
      assert Interval.between(start, stop, :second) == {:second, 10}
    end

    test "defaults to seconds if unit is not provided", %{start: start, stop: stop} do
      assert Interval.between(start, stop) == {:second, 10}
    end
  end

  describe "humanize/1" do
    test "humanizes interval with integer value" do
      assert Interval.humanize({:second, 10}) == "10s"
    end

    test "humanizes interval with float value" do
      assert Interval.humanize({:second, 10.5}) == "10.50s"
    end

    test "humanizes all time units with correct abbreviations" do
      # Use values that won't normalize to different units
      assert Interval.humanize({:nanosecond, 500}) == "500ns"
      assert Interval.humanize({:microsecond, 500}) == "500μs"
      assert Interval.humanize({:millisecond, 500}) == "500ms"
      assert Interval.humanize({:second, 30}) == "30s"
      assert Interval.humanize({:minute, 30}) == "30m"
      assert Interval.humanize({:hour, 12}) == "12h"
      assert Interval.humanize({:day, 3}) == "3d"
      assert Interval.humanize({:week, 2}) == "2w"
    end
  end

  describe "normalize/1" do
    test "scales down intervals correctly" do
      scale_down_cases = [
        {{:week, 0.5}, {:day, 3.5}},
        {{:day, 0.5}, {:hour, 12.0}},
        {{:hour, 0.5}, {:minute, 30.0}},
        {{:minute, 0.5}, {:second, 30.0}},
        {{:second, 0.5}, {:millisecond, 500.0}},
        {{:millisecond, 0.5}, {:microsecond, 500.0}},
        {{:microsecond, 0.5}, {:nanosecond, 500.0}}
      ]

      for {input, expected} <- scale_down_cases do
        assert Interval.normalize(input) == expected
      end
    end

    test "scales up intervals correctly" do
      scale_up_cases = [
        {{:nanosecond, 1000}, {:microsecond, 1}},
        {{:microsecond, 1000}, {:millisecond, 1}},
        {{:millisecond, 1000}, {:second, 1}},
        {{:second, 60}, {:minute, 1}},
        {{:minute, 60}, {:hour, 1}},
        {{:hour, 24}, {:day, 1}},
        {{:day, 7}, {:week, 1}}
      ]

      for {input, expected} <- scale_up_cases do
        assert Interval.normalize(input) == expected
      end
    end

    test "rounds float values correctly" do
      assert Interval.normalize({:second, 1.0}) == {:second, 1}
      assert Interval.normalize({:second, 1.5}) == {:second, 1.5}
    end
  end
end
