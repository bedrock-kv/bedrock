defmodule Bedrock.DataPlane.Log.Shale.ColdStartingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.ColdStarting
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Version

  @segment_dir "test/fixtures/segments"

  setup do
    File.mkdir_p!(@segment_dir)
    on_exit(fn -> File.rm_rf!(@segment_dir) end)
    :ok
  end

  describe "reload_segments_at_path/2" do
    test "returns segments sorted by version in descending order" do
      create_segment_file(1, 0)
      create_segment_file(2, 1)
      create_segment_file(10, 2)

      version_10 = Version.from_integer(10)
      version_2 = Version.from_integer(2)
      version_1 = Version.from_integer(1)

      assert {:ok,
              [
                %Segment{min_version: ^version_10, previous_version: ^version_2},
                %Segment{min_version: ^version_2, previous_version: ^version_1},
                %Segment{min_version: ^version_1, previous_version: previous_version}
              ]} = ColdStarting.reload_segments_at_path(@segment_dir)

      assert previous_version == Version.zero()
    end

    test "ignores non-matching files in directory" do
      create_segment_file(1, 0)
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

    test "fails closed for a legacy segment without a persisted predecessor" do
      path = Path.join(@segment_dir, Segment.encode_file_name(1))
      File.write!(path, <<"BED0", 0::128>>)

      assert {:error, {:unsupported_wal_format, ^path}} =
               ColdStarting.reload_segments_at_path(@segment_dir)
    end
  end

  defp create_segment_file(version, previous_version) do
    path = Path.join(@segment_dir, Segment.encode_file_name(version))
    File.write!(path, :binary.copy(<<0>>, 1024))
    {:ok, writer} = Writer.open(path, Version.from_integer(previous_version))
    :ok = Writer.close(writer)
  end
end
