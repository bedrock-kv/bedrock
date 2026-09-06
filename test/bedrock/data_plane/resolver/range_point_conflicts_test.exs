defmodule Bedrock.DataPlane.Resolver.RangePointConflictsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Version

  test "range reads detect newer point writes with half-open boundaries" do
    for point <- ["a", "b", "b\0", "c", "e", "f", "f\0", "g"],
        read_version <- [99, 100, 101] do
      conflicts = Conflicts.add_conflicts(Conflicts.new(), [{point, point <> <<0>>}], Version.from_integer(100))
      expected = if point >= "b" and point < "f" and read_version < 100, do: :abort, else: :ok

      assert Conflicts.check_conflicts(conflicts, [{"b", "f"}], Version.from_integer(read_version)) == expected,
             "point=#{inspect(point)} read_version=#{read_version}"
    end
  end
end
