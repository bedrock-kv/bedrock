defmodule Bedrock.DataPlane.Log.Shale.WriterTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Writer

  @test_file "test_segment.log"

  setup do
    File.write!(@test_file, :binary.copy(<<0>>, 1024))
    on_exit(fn -> File.rm(@test_file) end)
    {:ok, writer} = Writer.open(@test_file)
    %{writer: writer}
  end

  describe "open/1" do
    test "successfully opens a file and initializes the writer struct" do
      # Use pattern matching to assert all fields and that fd is present
      assert {:ok, %Writer{fd: fd, write_offset: 4, bytes_remaining: 1004, sync_fun: sync_fun}} =
               Writer.open(@test_file)

      assert fd
      assert is_function(sync_fun, 1)
    end

    test "uses custom sync function from options" do
      sync_fun = fn _fd -> :ok end

      assert {:ok, %Writer{sync_fun: writer_sync_fun}} =
               Writer.open(@test_file, sync_fun: sync_fun)

      assert writer_sync_fun == sync_fun
    end
  end

  describe "close/1" do
    test "successfully closes the file descriptor", %{writer: writer} do
      assert :ok = Writer.close(writer)
    end

    test "returns :ok when writer is nil" do
      assert :ok = Writer.close(nil)
    end
  end

  describe "append/2" do
    test "returns :segment_full error when there is not enough space", %{writer: writer} do
      large_transaction = :binary.copy(<<0>>, 1016)
      commit_version = <<1::unsigned-big-64>>
      assert {:error, :segment_full} = Writer.append(writer, large_transaction, commit_version)
    end

    test "successfully appends a transaction and updates the writer struct", %{writer: writer} do
      transaction = <<1, 2, 3, 4>>
      commit_version = <<1::unsigned-big-64>>

      # Use pattern matching to assert the exact structure and values
      assert {:ok, %Writer{write_offset: 24, bytes_remaining: 984}} =
               Writer.append(writer, transaction, commit_version)
    end

    test "returns error when writing to closed file descriptor", %{writer: writer} do
      # Close the file descriptor to cause pwrite to fail
      :ok = Writer.close(writer)

      transaction = <<1, 2, 3, 4>>
      commit_version = <<1::unsigned-big-64>>

      # Writing to a closed file should return an error
      assert {:error, _reason} = Writer.append(writer, transaction, commit_version)
    end

    test "returns sync error and does not advance offsets" do
      sync_fun = fn _fd -> {:error, :eio} end
      assert {:ok, writer} = Writer.open(@test_file, sync_fun: sync_fun)

      transaction = <<1, 2, 3, 4>>
      commit_version = <<1::unsigned-big-64>>

      assert {:error, :eio} = Writer.append(writer, transaction, commit_version)
      assert writer.write_offset == 4
      assert writer.bytes_remaining == 1004
      assert :ok = Writer.close(writer)
    end
  end
end
