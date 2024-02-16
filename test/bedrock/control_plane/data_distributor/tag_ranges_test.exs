defmodule Bedrock.ControlPlane.DataDistributor.TagRangesTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.DataDistributor.TagRanges

  describe "DataDistributor.TagRanges.new/0" do
    test "creates an instance" do
      assert team_list = TagRanges.new()
      assert team_list |> :ets.info()
    end
  end

  describe "DataDistributor.TagRanges.add_tag/2" do
    test "can add a non-overlapping team to the list" do
      team_list = TagRanges.new()
      assert :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert [{"a", "b", "1"}] = team_list |> :ets.tab2list()
    end

    test "can add a team that ends at the lower bound of another team" do
      team_list = TagRanges.new()
      assert :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert :ok = TagRanges.add_tag(team_list, "2", {<<>>, "a"})
    end

    test "can add a team that starts at the upper bound of another team" do
      team_list = TagRanges.new()
      assert :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert :ok = TagRanges.add_tag(team_list, "2", {"b", "bb"})
    end

    test "returns the proper error when attempting to add a team that would fall inside an existing team" do
      team_list = TagRanges.new()
      assert :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert {:error, :key_range_overlaps} = TagRanges.add_tag(team_list, "2", {"aa", "aaa"})
    end

    test "returns the proper error when attempting to add a team that overlaps the upper bound of an existing team" do
      team_list = TagRanges.new()
      assert :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert {:error, :key_range_overlaps} = TagRanges.add_tag(team_list, "2", {"aa", "bb"})
    end

    test "returns the proper error when attempting to add a team that overlaps the lower bound of an existing team" do
      team_list = TagRanges.new()
      assert :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert {:error, :key_range_overlaps} = TagRanges.add_tag(team_list, "2", {<<>>, "aa"})
    end
  end

  describe "DataDistributor.TagRanges.tag_for_key/2" do
    test "returns :error when no teams exist" do
      assert {:error, :not_found} = TagRanges.new() |> TagRanges.tag_for_key("foo")
    end

    test "returns the team when key is the first within the range of supported keys" do
      team_list = TagRanges.new()
      :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert {:ok, "1"} = team_list |> TagRanges.tag_for_key("a")
    end

    test "returns the team when key falls within the range of supported keys" do
      team_list = TagRanges.new()
      :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert {:ok, "1"} = team_list |> TagRanges.tag_for_key("aa")
    end

    test "returns an error when key is the highest key within the range of supported keys" do
      team_list = TagRanges.new()
      :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert {:error, :not_found} = team_list |> TagRanges.tag_for_key("b")
    end

    test "returns an error when the key falls below the range of supported keys" do
      team_list = TagRanges.new()
      :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert {:error, :not_found} = team_list |> TagRanges.tag_for_key(<<>>)
    end

    test "returns an error when the key falls above the range of supported keys" do
      team_list = TagRanges.new()
      :ok = TagRanges.add_tag(team_list, "1", {"a", "b"})
      assert {:error, :not_found} = team_list |> TagRanges.tag_for_key("bb")
    end
  end
end
