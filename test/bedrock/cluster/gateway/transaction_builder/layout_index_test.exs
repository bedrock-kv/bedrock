defmodule Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex

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

  describe "build_index/1" do
    test "builds index from overlapping storage teams" do
      layout = create_simple_layout()
      index = LayoutIndex.build_index(layout)

      assert %LayoutIndex{tree: tree} = index
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
      result = LayoutIndex.lookup_key!(index, "m")

      assert {_, pids} = result
      assert :up_pid in pids
      refute :down_server in pids
    end
  end

  describe "lookup_key/2" do
    test "finds storage servers for key in segmented range" do
      layout = create_simple_layout()
      index = LayoutIndex.build_index(layout)

      # Test key "c" - should be in segment with only pid1
      result = LayoutIndex.lookup_key!(index, "c")
      assert {_, pids} = result
      assert :pid1 in pids
      refute :pid2 in pids

      # Test key "e" - should be in overlapping segment with pid1 and pid2
      result = LayoutIndex.lookup_key!(index, "e")
      assert {_, pids} = result
      assert :pid1 in pids and :pid2 in pids

      # Test key "j" - should be in overlapping segment with pid2 and pid3
      result = LayoutIndex.lookup_key!(index, "j")
      assert {_, pids} = result
      assert :pid2 in pids and :pid3 in pids
    end

    test "raises for key outside any range" do
      layout = create_simple_layout()
      index = LayoutIndex.build_index(layout)

      # Test key "z" - outside all ranges
      assert_raise RuntimeError, ~r/No segment found containing key/, fn ->
        LayoutIndex.lookup_key!(index, "z")
      end
    end
  end

  describe "lookup_range/3" do
    test "finds all segments overlapping with query range" do
      layout = create_simple_layout()
      index = LayoutIndex.build_index(layout)

      # Query range "a" to "p" should cover all segments
      result = LayoutIndex.lookup_range(index, "a", "p")

      # Should return multiple segments
      assert length(result) > 1

      # All our PIDs should appear somewhere
      all_pids = result |> Enum.flat_map(fn {_, pids} -> pids end) |> Enum.uniq()
      assert :pid1 in all_pids
      assert :pid2 in all_pids
      assert :pid3 in all_pids
    end

    test "returns empty list for range outside all segments" do
      layout = create_simple_layout()
      index = LayoutIndex.build_index(layout)

      # Query range "x" to "z" - outside all ranges
      result = LayoutIndex.lookup_range(index, "x", "z")
      assert result == []
    end
  end

  describe "segmentation behavior" do
    test "creates non-overlapping segments from overlapping ranges" do
      layout = create_simple_layout()
      index = LayoutIndex.build_index(layout)

      # Get all segments by doing a very wide range query
      all_segments = LayoutIndex.lookup_range(index, "", "~")

      # Verify segments don't overlap - each segment end should be <= next segment start
      sorted_segments = Enum.sort_by(all_segments, fn {{start, _}, _} -> start end)

      segments_valid =
        sorted_segments
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.all?(fn [{{_start1, end1}, _}, {{start2, _end2}, _}] ->
          end1 <= start2 or end1 == :end
        end)

      assert segments_valid, "Segments should be non-overlapping"
    end
  end
end
