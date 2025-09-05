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

  test "reload_segments_at_path/2 returns sorted segments" do
    File.write!(Path.join(@segment_dir, Segment.encode_file_name(1)), "")
    File.write!(Path.join(@segment_dir, Segment.encode_file_name(2)), "")
    File.write!(Path.join(@segment_dir, Segment.encode_file_name(10)), "")

    {:ok, segments} = ColdStarting.reload_segments_at_path(@segment_dir)

    version_10 = Version.from_integer(10)
    version_2 = Version.from_integer(2)
    version_1 = Version.from_integer(1)

    assert [
             %Segment{min_version: ^version_10},
             %Segment{min_version: ^version_2},
             %Segment{min_version: ^version_1}
           ] =
             segments
  end

  test "reload_segments_at_path/2 ignores non-matching files" do
    File.write!(Path.join(@segment_dir, Segment.encode_file_name(1)), "")
    File.write!(Path.join(@segment_dir, "other_file.log"), "")

    {:ok, segments} = ColdStarting.reload_segments_at_path(@segment_dir)

    version_1 = Version.from_integer(1)
    assert [%Segment{min_version: ^version_1}] = segments
  end
end
