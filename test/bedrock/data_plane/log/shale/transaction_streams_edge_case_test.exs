defmodule Bedrock.DataPlane.Log.Shale.TransactionStreamsEdgeCaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.TransactionTestSupport
  alias Bedrock.DataPlane.Version

  @moduletag :tmp_dir

  describe "TransactionStreams.from_file!/1" do
    test "gracefully handles files with corrupted CRC32 checksums", %{tmp_dir: tmp_dir} do
      # Create a WAL file with invalid CRC32 to trigger line 114: true -> nil
      wal_file = Path.join(tmp_dir, "corrupted.wal")

      # WAL magic number
      magic = <<"BED0">>

      # Valid transaction data
      transaction = TransactionTestSupport.new_log_transaction(1, %{"key" => "value"})
      payload = transaction

      # Create entry with WRONG CRC32 (should be :erlang.crc32(payload) but we'll use 0)
      version_binary = Version.from_integer(1)
      size_in_bytes = byte_size(payload)
      # This will not match :erlang.crc32(payload)
      wrong_crc32 = 0

      entry =
        <<version_binary::binary-size(8), size_in_bytes::unsigned-big-32, payload::binary,
          wrong_crc32::unsigned-big-32>>

      # Write file with magic + corrupted entry
      File.write!(wal_file, magic <> entry)

      # Reading should handle the corruption gracefully
      stream = TransactionStreams.from_file!(wal_file)

      # The corrupted entry should be handled - when CRC fails, the function returns nil
      # which causes Stream.resource to halt the stream
      results =
        try do
          Enum.to_list(stream)
        rescue
          error -> {:error, error}
        catch
          :exit, reason -> {:exit, reason}
        end

      # The stream should halt when it encounters the corruption
      assert results == [] or match?({:error, _}, results) or match?({:exit, _}, results)
    end

    test "detects and reports truncated WAL file corruption", %{tmp_dir: tmp_dir} do
      # Create a WAL file with truncated data to trigger line 118-120: {_, offset} -> error
      wal_file = Path.join(tmp_dir, "truncated.wal")

      # WAL magic number
      magic = <<"BED0">>

      # Incomplete entry - just version and size, but no payload or CRC
      version_binary = Version.from_integer(1)
      # Claim 100 bytes but don't provide them
      size_in_bytes = 100

      incomplete_entry = <<version_binary::binary-size(8), size_in_bytes::unsigned-big-32>>

      # Write truncated file
      File.write!(wal_file, magic <> incomplete_entry)

      # Reading should detect the truncation
      stream = TransactionStreams.from_file!(wal_file)
      results = Enum.to_list(stream)

      # Should get a corruption error
      assert length(results) == 1
      assert {:error, {:corrupted, _offset}} = hd(results)
    end
  end

  describe "TransactionStreams.until_version/2" do
    test "returns stream unchanged when last_version is nil" do
      # This tests the specific nil guard clause: until_version(stream, nil), do: stream
      # This is a specific API boundary case not covered by property tests

      transactions = [
        create_encoded_tx(Version.from_integer(10)),
        create_encoded_tx(Version.from_integer(20)),
        create_encoded_tx(Version.from_integer(30))
      ]

      # Calling until_version with nil should return the same stream unchanged
      result_stream = TransactionStreams.until_version(transactions, nil)
      result_transactions = Enum.to_list(result_stream)

      assert result_transactions == transactions
    end
  end

  # Helper functions
  defp create_encoded_tx(version) do
    version_int = Version.to_integer(version)
    TransactionTestSupport.new_log_transaction(version_int, %{"key" => "value_#{version_int}"})
  end
end
