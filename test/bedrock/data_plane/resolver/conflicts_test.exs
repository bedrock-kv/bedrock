defmodule Bedrock.DataPlane.Resolver.ConflictsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Version

  defp v(i), do: Version.from_integer(i)

  describe "oldest_version floor" do
    test "new/0 has no floor: any read version checks cleanly against empty history" do
      assert :ok = Conflicts.check_conflicts(Conflicts.new(), [{"a", "a\0"}], v(0))
    end

    test "new/1 seeds the floor: reads below it abort even with empty history" do
      conflicts = Conflicts.new(v(100))

      assert :abort = Conflicts.check_conflicts(conflicts, [{"a", "a\0"}], v(99))
      assert :ok = Conflicts.check_conflicts(conflicts, [{"a", "a\0"}], v(100))
    end

    test "remove_old_conflicts advances the floor to the prune horizon" do
      conflicts =
        Conflicts.new()
        |> Conflicts.add_conflicts([{"k", "k\0"}], v(100))
        |> Conflicts.remove_old_conflicts(v(200))

      # The write at v100 was pruned; a read below the horizon can no longer be
      # validated and must abort rather than silently pass.
      assert :abort = Conflicts.check_conflicts(conflicts, [{"k", "k\0"}], v(50))

      # Even for keys never written: history below the floor is unknowable.
      assert :abort = Conflicts.check_conflicts(conflicts, [{"other", "other\0"}], v(199))

      # At or above the horizon all surviving entries are visible.
      assert :ok = Conflicts.check_conflicts(conflicts, [{"k", "k\0"}], v(200))
    end

    test "floor never regresses" do
      conflicts =
        Conflicts.new()
        |> Conflicts.remove_old_conflicts(v(200))
        |> Conflicts.remove_old_conflicts(v(100))

      assert :abort = Conflicts.check_conflicts(conflicts, [{"a", "a\0"}], v(150))
      assert :ok = Conflicts.check_conflicts(conflicts, [{"a", "a\0"}], v(200))
    end

    test "conflicts at or above the floor are still detected" do
      conflicts =
        Conflicts.new()
        |> Conflicts.add_conflicts([{"k", "k\0"}], v(300))
        |> Conflicts.remove_old_conflicts(v(200))

      assert :abort = Conflicts.check_conflicts(conflicts, [{"k", "k\0"}], v(250))
    end
  end
end
