defmodule Bedrock.DataPlane.Log.Shale.TransactionStreamsEdgeCaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @moduletag :tmp_dir

  describe "TransactionStreams.from_file!/1" do
    test "gracefully handles files with corrupted CRC32 checksums", %{tmp_dir: tmp_dir} do
      # Create a WAL file with invalid CRC32 to trigger corruption handling
      transaction = TransactionTestSupport.new_log_transaction(1, %{"key" => "value"})
      corrupted_entry = create_corrupted_entry(transaction)
      wal_file = create_wal_file_with_entry(tmp_dir, "corrupted.wal", corrupted_entry)

      # Reading should handle the corruption gracefully
      stream = TransactionStreams.from_file!(wal_file)

      # The corrupted entry should be handled - when CRC fails, the stream halts
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
      # Create a WAL file with truncated data to trigger truncation detection
      truncated_entry = create_truncated_entry()
      wal_file = create_wal_file_with_entry(tmp_dir, "truncated.wal", truncated_entry)

      # Reading should detect the truncation
      stream = TransactionStreams.from_file!(wal_file)
      results = Enum.to_list(stream)

      # Should get exactly one corruption error
      assert [{:error, {:corrupted, _offset}}] = results
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
    TransactionTestSupport.new_log_transaction(Version.to_integer(version), %{
      "key" => "value_#{Version.to_integer(version)}"
    })
  end

  defp create_wal_file_with_entry(tmp_dir, filename, entry_data) do
    wal_file = Path.join(tmp_dir, filename)
    magic = <<"BED0">>
    File.write!(wal_file, magic <> entry_data)
    wal_file
  end

  defp create_corrupted_entry(transaction) do
    version_binary = Version.from_integer(1)
    size_in_bytes = byte_size(transaction)
    # Wrong CRC32 to trigger corruption
    wrong_crc32 = 0

    <<version_binary::binary-size(8), size_in_bytes::unsigned-big-32, transaction::binary,
      wrong_crc32::unsigned-big-32>>
  end

  defp create_truncated_entry do
    version_binary = Version.from_integer(1)
    # Claim 100 bytes but don't provide them
    size_in_bytes = 100
    <<version_binary::binary-size(8), size_in_bytes::unsigned-big-32>>
  end
end
