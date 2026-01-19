defmodule Bedrock.DataPlane.Log.Shale.ColdStartingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.ColdStarting
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Version

  @segment_dir "test/fixtures/segments"

  setup do
    File.mkdir_p!(@segment_dir)
    on_exit(fn -> File.rm_rf!(@segment_dir) end)
    :ok
  end

  describe "reload_segments_at_path/2" do
    test "returns segments sorted by version in descending order" do
      create_segment_file(1)
      create_segment_file(2)
      create_segment_file(10)

      version_10 = Version.from_integer(10)
      version_2 = Version.from_integer(2)
      version_1 = Version.from_integer(1)

      assert {:ok,
              [
                %Segment{min_version: ^version_10},
                %Segment{min_version: ^version_2},
                %Segment{min_version: ^version_1}
              ]} = ColdStarting.reload_segments_at_path(@segment_dir)
    end

    test "ignores non-matching files in directory" do
      create_segment_file(1)
      File.write!(Path.join(@segment_dir, "other_file.log"), "")

      version_1 = Version.from_integer(1)

      assert {:ok, [%Segment{min_version: ^version_1}]} = ColdStarting.reload_segments_at_path(@segment_dir)
    end

    test "returns error with POSIX reason when unable to list directory" do
      # Use a path that doesn't exist or can't be read
      non_existent_path = "/non/existent/path/that/should/not/exist"

      assert {:error, {:unable_to_list_segments, posix}} =
               ColdStarting.reload_segments_at_path(non_existent_path)

      assert posix == :enoent
    end

    test "handles empty directory" do
      # Directory exists but has no segment files
      assert {:ok, []} = ColdStarting.reload_segments_at_path(@segment_dir)
    end
  end

  defp create_segment_file(version) do
    File.write!(Path.join(@segment_dir, Segment.encode_file_name(version)), "")
  end
end
