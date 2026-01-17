defmodule Bedrock.DataPlane.Storage.Olivine.CompactionWriterTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.CompactionWriter.Bundle
  alias Bedrock.DataPlane.Storage.Olivine.CompactionWriter.SplitFile

  describe "SplitFile writer" do
    @tag :tmp_dir
    setup context do
      tmp_dir =
        context[:tmp_dir] || Path.join(System.tmp_dir!(), "split_file_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      {:ok, tmp_dir: tmp_dir}
    end

    test "new/2 creates files and returns writer struct", %{tmp_dir: tmp_dir} do
      data_path = String.to_charlist(Path.join(tmp_dir, "data"))
      idx_path = String.to_charlist(Path.join(tmp_dir, "idx"))

      assert {:ok, writer} = SplitFile.new(data_path, idx_path)
      assert writer.data_path == data_path
      assert writer.idx_path == idx_path
      assert writer.data_offset == 0

      # Clean up
      :file.close(writer.data_fd)
      :file.close(writer.idx_fd)
    end

    test "write_data/2 writes to data file and updates offset", %{tmp_dir: tmp_dir} do
      data_path = String.to_charlist(Path.join(tmp_dir, "data"))
      idx_path = String.to_charlist(Path.join(tmp_dir, "idx"))

      {:ok, writer} = SplitFile.new(data_path, idx_path)

      data = "hello world"
      {:ok, writer} = SplitFile.write_data(writer, data)
      assert writer.data_offset == byte_size(data)

      # Write more data
      more_data = "more data"
      {:ok, writer} = SplitFile.write_data(writer, more_data)
      assert writer.data_offset == byte_size(data) + byte_size(more_data)

      # Clean up
      :file.close(writer.data_fd)
      :file.close(writer.idx_fd)
    end

    test "write_index/2 writes to index file", %{tmp_dir: tmp_dir} do
      data_path = String.to_charlist(Path.join(tmp_dir, "data"))
      idx_path = String.to_charlist(Path.join(tmp_dir, "idx"))

      {:ok, writer} = SplitFile.new(data_path, idx_path)

      index_data = "index record"
      {:ok, _writer} = SplitFile.write_index(writer, index_data)

      # Verify by reading back
      :file.close(writer.idx_fd)
      {:ok, content} = File.read(to_string(idx_path))
      assert content == index_data

      :file.close(writer.data_fd)
    end

    test "finish/1 syncs files and returns result", %{tmp_dir: tmp_dir} do
      data_path = String.to_charlist(Path.join(tmp_dir, "data"))
      idx_path = String.to_charlist(Path.join(tmp_dir, "idx"))

      {:ok, writer} = SplitFile.new(data_path, idx_path)

      {:ok, writer} = SplitFile.write_data(writer, "data content")
      {:ok, writer} = SplitFile.write_index(writer, "index content")
      {:ok, result} = SplitFile.finish(writer)

      assert result.data_path == data_path
      assert result.idx_path == idx_path
      assert result.data_offset == byte_size("data content")
      assert result.idx_offset == byte_size("index content")

      # Verify files have content
      {:ok, data_content} = File.read(to_string(data_path))
      {:ok, idx_content} = File.read(to_string(idx_path))
      assert data_content == "data content"
      assert idx_content == "index content"

      :file.close(result.data_fd)
      :file.close(result.idx_fd)
    end

    test "write_data/2 handles iodata (lists)", %{tmp_dir: tmp_dir} do
      data_path = String.to_charlist(Path.join(tmp_dir, "data"))
      idx_path = String.to_charlist(Path.join(tmp_dir, "idx"))

      {:ok, writer} = SplitFile.new(data_path, idx_path)

      # Write iodata as list
      iodata = ["hello", " ", "world"]
      {:ok, writer} = SplitFile.write_data(writer, iodata)
      assert writer.data_offset == 11

      {:ok, result} = SplitFile.finish(writer)

      {:ok, content} = File.read(to_string(data_path))
      assert content == "hello world"

      :file.close(result.data_fd)
      :file.close(result.idx_fd)
    end
  end

  describe "Bundle writer" do
    @tag :tmp_dir
    setup context do
      tmp_dir =
        context[:tmp_dir] || Path.join(System.tmp_dir!(), "bundle_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      {:ok, tmp_dir: tmp_dir}
    end

    test "new/1 creates file and returns writer struct", %{tmp_dir: tmp_dir} do
      path = String.to_charlist(Path.join(tmp_dir, "bundle"))

      assert {:ok, writer} = Bundle.new(path)
      assert writer.path == path
      assert writer.data_end_offset == 0

      :file.close(writer.fd)
    end

    test "write_data/2 writes and tracks data_end_offset", %{tmp_dir: tmp_dir} do
      path = String.to_charlist(Path.join(tmp_dir, "bundle"))

      {:ok, writer} = Bundle.new(path)

      data = "data bytes here"
      {:ok, writer} = Bundle.write_data(writer, data)
      assert writer.data_end_offset == byte_size(data)

      :file.close(writer.fd)
    end

    test "write_index/2 appends index after data", %{tmp_dir: tmp_dir} do
      path = String.to_charlist(Path.join(tmp_dir, "bundle"))

      {:ok, writer} = Bundle.new(path)

      # Write data first
      {:ok, writer} = Bundle.write_data(writer, "data")
      data_end = writer.data_end_offset

      # Write index
      {:ok, writer} = Bundle.write_index(writer, "index")

      # data_end_offset should not change after writing index
      assert writer.data_end_offset == data_end

      :file.close(writer.fd)

      # Verify bundle contains both
      {:ok, content} = File.read(to_string(path))
      assert content == "dataindex"
    end

    test "finish/1 returns result with offsets", %{tmp_dir: tmp_dir} do
      path = String.to_charlist(Path.join(tmp_dir, "bundle"))

      {:ok, writer} = Bundle.new(path)

      {:ok, writer} = Bundle.write_data(writer, "data portion")
      {:ok, writer} = Bundle.write_index(writer, "index portion")
      {:ok, result} = Bundle.finish(writer)

      assert result.path == path
      assert result.data_end_offset == byte_size("data portion")
      assert result.total_size == byte_size("data portion") + byte_size("index portion")

      :file.close(result.fd)
    end

    test "bundle format: data followed by index", %{tmp_dir: tmp_dir} do
      path = String.to_charlist(Path.join(tmp_dir, "bundle"))

      {:ok, writer} = Bundle.new(path)

      data = "AAAAAAAAAA"
      index = "IIII"

      {:ok, writer} = Bundle.write_data(writer, data)
      {:ok, writer} = Bundle.write_index(writer, index)
      {:ok, result} = Bundle.finish(writer)

      # Verify the bundle layout
      {:ok, content} = File.read(to_string(path))
      assert content == data <> index
      assert result.data_end_offset == byte_size(data)
      assert result.total_size == byte_size(data) + byte_size(index)

      :file.close(result.fd)
    end
  end
end
