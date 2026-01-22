defmodule Bedrock.ObjectStorage.SnapshotBundleTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.IndexDatabase
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

  # Shared setup for file-based tests
  defp setup_tmp_dir(context) do
    tmp_dir =
      context[:tmp_dir] ||
        Path.join(System.tmp_dir!(), "snapshot_bundle_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp_dir)

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  describe "create/3" do
    setup :setup_tmp_dir

    test "creates bundle from data and idx files", %{tmp_dir: tmp_dir} do
      data = "this is the data portion"
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      data_path = Path.join(tmp_dir, "source.data")
      idx_path = Path.join(tmp_dir, "source.idx")
      bundle_path = Path.join(tmp_dir, "output.bundle")

      File.write!(data_path, data)
      File.write!(idx_path, index_binary)

      assert {:ok, bundle_size} = SnapshotBundle.create(data_path, idx_path, bundle_path)
      assert bundle_size == byte_size(data) + byte_size(index_binary)

      # Verify bundle contains data followed by index
      bundle_content = File.read!(bundle_path)
      assert bundle_content == data <> index_binary
    end

    test "create and split are inverse operations", %{tmp_dir: tmp_dir} do
      data = String.duplicate("D", 500)
      version = <<0, 0, 0, 0, 0, 0, 0, 7>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      # Start with separate files
      data_path = Path.join(tmp_dir, "original.data")
      idx_path = Path.join(tmp_dir, "original.idx")
      bundle_path = Path.join(tmp_dir, "roundtrip.bundle")
      split_data_path = Path.join(tmp_dir, "split.data")
      split_idx_path = Path.join(tmp_dir, "split.idx")

      File.write!(data_path, data)
      File.write!(idx_path, index_binary)

      # Create bundle
      {:ok, _} = SnapshotBundle.create(data_path, idx_path, bundle_path)

      # Split bundle
      {:ok, _, _} = SnapshotBundle.split(bundle_path, split_data_path, split_idx_path)

      # Verify round-trip preserves content
      assert File.read!(split_data_path) == data
      assert File.read!(split_idx_path) == index_binary
    end

    test "returns error for non-existent data file", %{tmp_dir: tmp_dir} do
      idx_path = Path.join(tmp_dir, "exists.idx")
      bundle_path = Path.join(tmp_dir, "output.bundle")

      File.write!(idx_path, "index content")

      assert {:error, :enoent} =
               SnapshotBundle.create(
                 Path.join(tmp_dir, "nonexistent.data"),
                 idx_path,
                 bundle_path
               )
    end

    test "returns error for non-existent idx file", %{tmp_dir: tmp_dir} do
      data_path = Path.join(tmp_dir, "exists.data")
      bundle_path = Path.join(tmp_dir, "output.bundle")

      File.write!(data_path, "data content")

      assert {:error, :enoent} =
               SnapshotBundle.create(
                 data_path,
                 Path.join(tmp_dir, "nonexistent.idx"),
                 bundle_path
               )
    end
  end

  describe "split/3" do
    setup :setup_tmp_dir

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

  describe "split_in_place/3" do
    setup :setup_tmp_dir

    test "splits bundle in place without copying data", %{tmp_dir: tmp_dir} do
      data = "this is the data portion of the bundle"
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_path = Path.join(tmp_dir, "test.bundle")
      data_path = Path.join(tmp_dir, "test.data")
      idx_path = Path.join(tmp_dir, "test.idx")

      File.write!(bundle_path, data <> index_binary)

      assert {:ok, data_size, idx_size} = SnapshotBundle.split_in_place(bundle_path, data_path, idx_path)
      assert data_size == byte_size(data)
      assert idx_size == byte_size(index_binary)

      # Verify file contents
      assert File.read!(data_path) == data
      assert File.read!(idx_path) == index_binary

      # Bundle file should no longer exist (was renamed to data_path)
      refute File.exists?(bundle_path)
    end

    test "consumes bundle file (renamed to data path)", %{tmp_dir: tmp_dir} do
      data = String.duplicate("D", 1000)
      version = <<0, 0, 0, 0, 0, 0, 0, 5>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_path = Path.join(tmp_dir, "consume.bundle")
      data_path = Path.join(tmp_dir, "consume.data")
      idx_path = Path.join(tmp_dir, "consume.idx")

      File.write!(bundle_path, data <> index_binary)

      # Get inode of bundle file before split
      {:ok, %{inode: bundle_inode}} = File.stat(bundle_path)

      {:ok, _, _} = SnapshotBundle.split_in_place(bundle_path, data_path, idx_path)

      # Data file should have the same inode (was renamed, not copied)
      {:ok, %{inode: data_inode}} = File.stat(data_path)
      assert data_inode == bundle_inode
    end

    test "produces same result as split/3", %{tmp_dir: tmp_dir} do
      data = String.duplicate("X", 500)
      version = <<0, 0, 0, 0, 0, 0, 0, 7>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_content = data <> index_binary

      # Use split/3 for reference
      bundle1_path = Path.join(tmp_dir, "ref.bundle")
      data1_path = Path.join(tmp_dir, "ref.data")
      idx1_path = Path.join(tmp_dir, "ref.idx")

      File.write!(bundle1_path, bundle_content)
      {:ok, ref_data_size, ref_idx_size} = SnapshotBundle.split(bundle1_path, data1_path, idx1_path)

      # Use split_in_place/3
      bundle2_path = Path.join(tmp_dir, "inplace.bundle")
      data2_path = Path.join(tmp_dir, "inplace.data")
      idx2_path = Path.join(tmp_dir, "inplace.idx")

      File.write!(bundle2_path, bundle_content)
      {:ok, inplace_data_size, inplace_idx_size} = SnapshotBundle.split_in_place(bundle2_path, data2_path, idx2_path)

      # Same sizes
      assert inplace_data_size == ref_data_size
      assert inplace_idx_size == ref_idx_size

      # Same content
      assert File.read!(data2_path) == File.read!(data1_path)
      assert File.read!(idx2_path) == File.read!(idx1_path)
    end

    test "returns error for non-existent bundle", %{tmp_dir: tmp_dir} do
      bundle_path = Path.join(tmp_dir, "nonexistent.bundle")
      data_path = Path.join(tmp_dir, "data")
      idx_path = Path.join(tmp_dir, "idx")

      assert {:error, :enoent} = SnapshotBundle.split_in_place(bundle_path, data_path, idx_path)
    end

    test "splits bundle with empty data section", %{tmp_dir: tmp_dir} do
      version = <<0, 0, 0, 0, 0, 0, 0, 99>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_path = Path.join(tmp_dir, "empty_data.bundle")
      data_path = Path.join(tmp_dir, "empty.data")
      idx_path = Path.join(tmp_dir, "empty.idx")

      File.write!(bundle_path, index_binary)

      assert {:ok, data_size, idx_size} = SnapshotBundle.split_in_place(bundle_path, data_path, idx_path)
      assert data_size == 0
      assert idx_size == byte_size(index_binary)

      assert File.read!(data_path) == ""
      assert File.read!(idx_path) == index_binary
    end
  end

  describe "find_index_boundary_from_file/2" do
    setup :setup_tmp_dir

    test "finds boundary without reading entire file", %{tmp_dir: tmp_dir} do
      data = String.duplicate("Y", 10_000)
      version = <<0, 0, 0, 0, 0, 0, 0, 42>>
      pages_map = %{}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_path = Path.join(tmp_dir, "large.bundle")
      File.write!(bundle_path, data <> index_binary)

      {:ok, %{size: file_size}} = File.stat(bundle_path)

      assert {:ok, data_end, idx_size} = SnapshotBundle.find_index_boundary_from_file(bundle_path, file_size)
      assert data_end == byte_size(data)
      assert idx_size == byte_size(index_binary)
    end

    test "returns error for file too small", %{tmp_dir: tmp_dir} do
      bundle_path = Path.join(tmp_dir, "tiny.bundle")
      File.write!(bundle_path, "tiny")

      {:ok, %{size: file_size}} = File.stat(bundle_path)

      assert {:error, :invalid_bundle} = SnapshotBundle.find_index_boundary_from_file(bundle_path, file_size)
    end

    test "returns error for invalid magic number", %{tmp_dir: tmp_dir} do
      # Create a file with valid footer size but wrong magic
      fake_index = <<0xDEADBEEF::32, 0::64, 5::32, "hello", 5::32>>
      bundle_path = Path.join(tmp_dir, "bad_magic.bundle")

      File.write!(bundle_path, "some data" <> fake_index)

      {:ok, %{size: file_size}} = File.stat(bundle_path)

      assert {:error, :no_index_record} = SnapshotBundle.find_index_boundary_from_file(bundle_path, file_size)
    end
  end
end
