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
      assert {:ok, %Writer{fd: fd, write_offset: 4, bytes_remaining: 1004}} =
               Writer.open(@test_file)

      assert fd
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
  end
end
