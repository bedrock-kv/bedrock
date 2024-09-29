defmodule Bedrock.DataPlane.TransactionLog.Limestone.SegmentTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.TransactionLog.Limestone.Segment

  setup context do
    context
    |> Enum.reduce(context, fn
      {:with_10kib_segment, true}, context ->
        file_name = Path.join(context[:tmp_dir], UUID.uuid4())
        {:ok, segment} = Segment.allocate(file_name, 10_000)
        context |> Map.put(:segment, segment)

      _, context ->
        context
    end)
  end

  describe "Limestone.Segment" do
    @tag :tmp_dir
    test "allocate/2 successfully creates a segment file of the correct size", %{tmp_dir: tmp_dir} do
      file_name = Path.join(tmp_dir, UUID.uuid4())
      expected_size = 1024

      # Make sure the file doesn't exist before we start.
      refute File.exists?(file_name)

      # Allocate a new segment.
      assert {:ok, %Segment{path: ^file_name, size: ^expected_size}} =
               Segment.allocate(file_name, expected_size)

      # Does the file exist now? Is it the right size?
      assert File.exists?(file_name)
      assert File.stat!(file_name).size == expected_size
    end

    @tag :tmp_dir
    @tag :with_10kib_segment
    test "from_path/1 successfully creates a segment from an existing file", %{segment: segment} do
      expected_path = segment.path
      assert {:ok, %Segment{path: ^expected_path, size: 10_000}} = Segment.from_path(segment.path)
    end

    @tag :tmp_dir
    @tag :with_10kib_segment
    test "writer!/1 returns an appropriately configured Writer", %{
      segment: segment
    } do
      assert {:ok, writer} = Segment.writer(segment)

      assert %Segment.Writer{
               fd: _fd,
               write_offset: write_offset,
               bytes_remaining: bytes_remaining
             } = writer

      assert 8 == write_offset
      assert segment.size - 8 == bytes_remaining

      # Is it still the right size?
      assert File.stat!(segment.path).size == segment.size
    end

    @tag :tmp_dir
    @tag :with_10kib_segment
    test "append/2 writes entries to the log correctly, and those entries can be read-back", %{
      segment: segment
    } do
      assert {:ok, writer} = Segment.writer(segment)

      assert {:ok, {1, 0}, writer} = Segment.append(writer, [{"foo", "bar"}])
      assert {:ok, {1, 0}, writer} = Segment.append(writer, [{"baz", "biz"}])
      assert {:ok, {2, 0}, writer} = Segment.append(writer, [{"abc", "def"}, {"ghi", "jkl"}])
      writer |> Segment.close()

      assert [
               {"foo", "bar"},
               {"baz", "biz"},
               {"abc", "def"},
               {"ghi", "jkl"}
             ] == Segment.stream!(segment) |> Enum.to_list()
    end
  end
end
