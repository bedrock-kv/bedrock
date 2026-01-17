defmodule Bedrock.ObjectStorage.SnapshotBundleTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase
  alias Bedrock.ObjectStorage.SnapshotBundle

  # IndexDatabase constants
  @magic_number 0x4F4C5644
  @header_size 16
  @footer_size 4

  describe "find_index_boundary/1" do
    test "finds boundary for valid bundle" do
      data = "test data content here"
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle = data <> index_binary

      assert {:ok, data_end, idx_size} = SnapshotBundle.find_index_boundary(bundle)
      assert data_end == byte_size(data)
      assert idx_size == byte_size(index_binary)
    end

    test "finds boundary for bundle with empty pages" do
      data = String.duplicate("X", 1000)
      version = <<1, 2, 3, 4, 5, 6, 7, 8>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle = data <> index_binary

      assert {:ok, data_end, idx_size} = SnapshotBundle.find_index_boundary(bundle)
      assert data_end == 1000
      assert idx_size == byte_size(index_binary)
    end

    test "returns error for bundle with invalid magic number" do
      # Create a bundle with wrong magic number
      data = "some data"
      fake_index = <<0xDEADBEEF::32, 0::64, 5::32, "hello", 5::32>>

      bundle = data <> fake_index

      assert {:error, :no_index_record} = SnapshotBundle.find_index_boundary(bundle)
    end

    test "returns error for bundle too small" do
      # Bundle smaller than minimum record size
      bundle = "tiny"

      assert {:error, :invalid_bundle} = SnapshotBundle.find_index_boundary(bundle)
    end

    test "handles bundle with only index record (no data)" do
      version = <<0, 0, 0, 0, 0, 0, 0, 5>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      bundle = IO.iodata_to_binary(index_record)

      assert {:ok, data_end, idx_size} = SnapshotBundle.find_index_boundary(bundle)
      assert data_end == 0
      assert idx_size == byte_size(bundle)
    end
  end

  describe "split/3" do
    @tag :tmp_dir
    setup context do
      tmp_dir =
        context[:tmp_dir] ||
          Path.join(System.tmp_dir!(), "snapshot_bundle_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      {:ok, tmp_dir: tmp_dir}
    end

    test "splits bundle into data and idx files", %{tmp_dir: tmp_dir} do
      # Create a bundle
      data = "this is the data portion of the bundle"
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_path = Path.join(tmp_dir, "test.bundle")
      data_path = Path.join(tmp_dir, "test.data")
      idx_path = Path.join(tmp_dir, "test.idx")

      File.write!(bundle_path, data <> index_binary)

      assert {:ok, data_size, idx_size} = SnapshotBundle.split(bundle_path, data_path, idx_path)
      assert data_size == byte_size(data)
      assert idx_size == byte_size(index_binary)

      # Verify file contents
      assert File.read!(data_path) == data
      assert File.read!(idx_path) == index_binary
    end

    test "split files can be read by IndexDatabase", %{tmp_dir: tmp_dir} do
      # Create meaningful data
      data_content = String.duplicate("value", 100)
      version = <<0, 0, 0, 0, 0, 0, 0, 42>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_path = Path.join(tmp_dir, "readable.bundle")
      data_path = Path.join(tmp_dir, "readable.data")
      idx_path = Path.join(tmp_dir, "readable.idx")

      File.write!(bundle_path, data_content <> index_binary)

      {:ok, _data_size, _idx_size} = SnapshotBundle.split(bundle_path, data_path, idx_path)

      # Verify we can read back the index file and find the version
      {:ok, fd} = :file.open(String.to_charlist(idx_path), [:read, :raw, :binary])
      {:ok, file_size} = :file.position(fd, {:eof, 0})

      # Read footer to get payload size
      {:ok, <<payload_size::32>>} = :file.pread(fd, file_size - @footer_size, @footer_size)

      # Read header
      {:ok, <<magic::32, read_version::binary-size(8), ^payload_size::32>>} = :file.pread(fd, 0, @header_size)

      assert magic == @magic_number
      assert read_version == version

      :file.close(fd)
    end

    test "returns error for non-existent bundle", %{tmp_dir: tmp_dir} do
      bundle_path = Path.join(tmp_dir, "nonexistent.bundle")
      data_path = Path.join(tmp_dir, "data")
      idx_path = Path.join(tmp_dir, "idx")

      assert {:error, :enoent} = SnapshotBundle.split(bundle_path, data_path, idx_path)
    end

    test "splits bundle with empty data section", %{tmp_dir: tmp_dir} do
      version = <<0, 0, 0, 0, 0, 0, 0, 99>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_path = Path.join(tmp_dir, "empty_data.bundle")
      data_path = Path.join(tmp_dir, "empty.data")
      idx_path = Path.join(tmp_dir, "empty.idx")

      # Bundle with only index, no data
      File.write!(bundle_path, index_binary)

      assert {:ok, data_size, idx_size} = SnapshotBundle.split(bundle_path, data_path, idx_path)
      assert data_size == 0
      assert idx_size == byte_size(index_binary)

      assert File.read!(data_path) == ""
      assert File.read!(idx_path) == index_binary
    end
  end
end
