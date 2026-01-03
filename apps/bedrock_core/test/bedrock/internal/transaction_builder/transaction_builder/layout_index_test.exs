defmodule Bedrock.Internal.TransactionBuilder.LayoutIndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex

  defp create_simple_layout do
    %{
      storage_teams: [
        %{
          key_range: {"a", "f"},
          storage_ids: ["storage1"]
        },
        %{
          key_range: {"d", "m"},
          storage_ids: ["storage2"]
        },
        %{
          key_range: {"h", "p"},
          storage_ids: ["storage3"]
        }
      ],
      services: %{
        "storage1" => %{kind: :storage, status: {:up, :pid1}},
        "storage2" => %{kind: :storage, status: {:up, :pid2}},
        "storage3" => %{kind: :storage, status: {:up, :pid3}}
      }
    }
  end

  defp build_simple_index, do: LayoutIndex.build_index(create_simple_layout())

  describe "build_index/1" do
    test "builds index from overlapping storage teams" do
      assert %LayoutIndex{tree: tree} = build_simple_index()
      refute :gb_trees.is_empty(tree)
    end

    test "filters out down storage servers" do
      layout = %{
        storage_teams: [
          %{
            key_range: {"a", "z"},
            storage_ids: ["up_server", "down_server"]
          }
        ],
        services: %{
          "up_server" => %{kind: :storage, status: {:up, :up_pid}},
          "down_server" => %{kind: :storage, status: {:down, nil}}
        }
      }

      index = LayoutIndex.build_index(layout)

      assert {_, pids} = LayoutIndex.lookup_key!(index, "m")
      assert :up_pid in pids
      refute :down_server in pids
    end
  end

  describe "lookup_key/2" do
    test "finds storage servers for key in segmented range" do
      index = build_simple_index()

      # Test key "c" - should be in segment with only pid1
      assert {_, pids} = LayoutIndex.lookup_key!(index, "c")
      assert :pid1 in pids
      refute :pid2 in pids

      # Test key "e" - should be in overlapping segment with pid1 and pid2
      assert {_, pids} = LayoutIndex.lookup_key!(index, "e")
      assert :pid1 in pids and :pid2 in pids

      # Test key "j" - should be in overlapping segment with pid2 and pid3
      assert {_, pids} = LayoutIndex.lookup_key!(index, "j")
      assert :pid2 in pids and :pid3 in pids
    end

    test "raises for key outside any range" do
      index = build_simple_index()

      assert_raise RuntimeError, ~r/No segment found containing key/, fn ->
        LayoutIndex.lookup_key!(index, "z")
      end
    end
  end

  describe "lookup_range/3" do
    test "finds all segments overlapping with query range" do
      index = build_simple_index()

      result = LayoutIndex.lookup_range(index, "a", "p")
      assert length(result) > 1

      # All our PIDs should appear somewhere
      all_pids = result |> Enum.flat_map(&elem(&1, 1)) |> Enum.uniq()

      for expected_pid <- [:pid1, :pid2, :pid3] do
        assert expected_pid in all_pids
      end
    end

    test "returns empty list for range outside all segments" do
      index = build_simple_index()

      assert [] = LayoutIndex.lookup_range(index, "x", "z")
    end
  end

  describe "segmentation behavior" do
    test "creates non-overlapping segments from overlapping ranges" do
      index = build_simple_index()
      all_segments = LayoutIndex.lookup_range(index, "", "~")

      # Verify segments don't overlap - each segment end should be <= next segment start
      segments_valid =
        all_segments
        |> Enum.sort_by(fn {{start, _}, _} -> start end)
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.all?(fn [{{_start1, end1}, _}, {{start2, _end2}, _}] ->
          end1 <= start2 or end1 == <<0xFF, 0xFF>>
        end)

      assert segments_valid, "Segments should be non-overlapping"
    end
  end
end
