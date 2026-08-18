defmodule Bedrock.DataPlane.Log.Shale.ColdStartingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.ColdStarting
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

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

    test "an unlistable directory is a WAL I/O error carrying the path and POSIX cause" do
      non_existent_path = "/non/existent/path/that/should/not/exist"

      assert {:error, {:wal_io, ^non_existent_path, :enoent}} =
               ColdStarting.reload_segments_at_path(non_existent_path)
    end

    test "handles empty directory" do
      # Directory exists but has no segment files
      assert {:ok, []} = ColdStarting.reload_segments_at_path(@segment_dir)
    end

    test "loads a non-empty BED0 segment with a synthetic exclusive cursor" do
      path = create_legacy_segment_file(41)
      first_version = Version.from_integer(41)
      expected_cursor = Version.from_integer(40)

      assert {:ok,
              [
                %Segment{
                  path: ^path,
                  min_version: ^first_version,
                  previous_version: ^expected_cursor
                }
              ]} = ColdStarting.reload_segments_at_path(@segment_dir)
    end

    test "fails closed for an empty legacy segment" do
      path = Path.join(@segment_dir, Segment.encode_file_name(1))
      eof_marker = <<0xFFFFFFFFFFFFFFFF::unsigned-big-64, 0::unsigned-big-32, 0::unsigned-big-32>>
      File.write!(path, <<"BED0", eof_marker::binary>>)

      assert {:error, {:wal_format, ^path, :unsupported_wal_format}} =
               ColdStarting.reload_segments_at_path(@segment_dir)
    end

    test "fails closed when a BED0 filename disagrees with its first transaction" do
      first_version = Version.from_integer(41)
      transaction = TransactionTestSupport.new_log_transaction(41, %{"key" => "value"})

      entry =
        <<first_version::binary, byte_size(transaction)::unsigned-big-32, transaction::binary,
          :erlang.crc32(transaction)::unsigned-big-32>>

      path = Path.join(@segment_dir, Segment.encode_file_name(42))
      File.write!(path, <<"BED0", entry::binary>>)

      assert {:error, {:wal_format, ^path, :invalid_wal_format}} =
               ColdStarting.reload_segments_at_path(@segment_dir)
    end

    test "malformed bytes are a format error, never an I/O error" do
      path = Path.join(@segment_dir, Segment.encode_file_name(1))
      File.write!(path, "definitely not a WAL segment")

      assert {:error, {:wal_format, ^path, :invalid_wal_format}} =
               ColdStarting.reload_segments_at_path(@segment_dir)
    end
  end

  describe "Segment.from_path/1 error classification" do
    test "a missing file is a WAL I/O error with its POSIX cause, not format corruption" do
      path = Path.join(@segment_dir, Segment.encode_file_name(7))

      assert {:error, {:wal_io, ^path, :enoent}} = Segment.from_path(path)
    end
  end

  defp create_segment_file(version, previous_version) do
    path = Path.join(@segment_dir, Segment.encode_file_name(version))
    File.write!(path, :binary.copy(<<0>>, 1024))
    {:ok, writer} = Writer.open(path, Version.from_integer(previous_version))
    :ok = Writer.close(writer)
  end

  defp create_legacy_segment_file(version) do
    path = Path.join(@segment_dir, Segment.encode_file_name(version))
    commit_version = Version.from_integer(version)
    transaction = TransactionTestSupport.new_log_transaction(version, %{"key" => "value"})
    crc32 = :erlang.crc32(transaction)

    entry =
      <<commit_version::binary, byte_size(transaction)::unsigned-big-32, transaction::binary, crc32::unsigned-big-32>>

    eof_marker = <<0xFFFFFFFFFFFFFFFF::unsigned-big-64, 0::unsigned-big-32, 0::unsigned-big-32>>
    File.write!(path, <<"BED0", entry::binary, eof_marker::binary>>)
    path
  end
end
