defmodule Bedrock.DataPlane.Log.Shale.WalFormatTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.WalFormat
  alias Bedrock.DataPlane.Version

  describe "decode/1" do
    test "reads the persisted replay cursor from BED1" do
      previous_version = Version.from_integer(41)

      assert {:ok,
              %WalFormat{
                version: :bed1,
                header_size: 12,
                previous_version: ^previous_version
              }} = WalFormat.decode(<<"BED1", previous_version::binary, 0::128>>)
    end

    test "derives a synthetic exclusive cursor from the first BED0 transaction" do
      first_version = Version.from_integer(41)
      expected_cursor = Version.from_integer(40)

      assert {:ok,
              %WalFormat{
                version: :bed0,
                header_size: 4,
                previous_version: ^expected_cursor,
                first_version: ^first_version
              }} = WalFormat.decode(<<"BED0", first_version::binary, 0::64>>)
    end

    test "keeps the synthetic BED0 cursor at zero without underflow" do
      zero = Version.zero()

      assert {:ok, %WalFormat{version: :bed0, previous_version: ^zero}} =
               WalFormat.decode(<<"BED0", zero::binary, 0::64>>)
    end

    test "rejects an empty BED0 segment because it has no derivable cursor" do
      eof_marker = <<0xFFFFFFFFFFFFFFFF::unsigned-big-64, 0::unsigned-big-32, 0::unsigned-big-32>>

      assert {:error, :unsupported_wal_format} = WalFormat.decode(<<"BED0", eof_marker::binary>>)
    end

    test "rejects malformed and unknown headers" do
      assert {:error, :invalid_wal_format} = WalFormat.decode("BED0")
      assert {:error, :invalid_wal_format} = WalFormat.decode(<<"NOPE", 0::128>>)
    end
  end
end
