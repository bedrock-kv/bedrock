defmodule Bedrock.DataPlane.Log.Shale.WriterTest do
  use ExUnit.Case, async: true
  alias Bedrock.DataPlane.Log.Shale.Writer

  @test_file "test_segment.log"

  setup do
    File.write!(@test_file, :binary.copy(<<0>>, 1024))
    on_exit(fn -> File.rm(@test_file) end)
    :ok
  end

  test "open/1 successfully opens a file and initializes the writer struct" do
    assert {:ok, %Writer{fd: fd, write_offset: 4, bytes_remaining: 1004}} =
             Writer.open(@test_file)

    assert not is_nil(fd)
  end

  test "close/1 successfully closes the file descriptor" do
    {:ok, writer} = Writer.open(@test_file)
    assert :ok = Writer.close(writer)
  end

  test "close/1 returns :ok when writer is nil" do
    assert :ok = Writer.close(nil)
  end

  test "append/2 returns :segment_full error when there is not enough space" do
    {:ok, writer} = Writer.open(@test_file)
    large_transaction = :binary.copy(<<0>>, 1016)
    assert {:error, :segment_full} = Writer.append(writer, large_transaction)
  end

  test "append/2 successfully appends a transaction and updates the writer struct" do
    {:ok, writer} = Writer.open(@test_file)
    transaction = <<1, 2, 3, 4>>

    assert {:ok, %Writer{write_offset: 8, bytes_remaining: 1000}} =
             Writer.append(writer, transaction)
  end
end
