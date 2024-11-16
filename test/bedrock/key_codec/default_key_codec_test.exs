defmodule Bedrock.KeyCodec.DefaultKeyCodecTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Bedrock.KeyCodec.DefaultKeyCodec

  property "encode_key/1 returns {:ok, key} for binary keys" do
    check all(key <- binary()) do
      assert DefaultKeyCodec.encode_key(key) == {:ok, key}
    end
  end

  property "encode_key/1 returns {:error, :invalid_key} for non-binary keys" do
    check all(key <- term()) do
      if not is_binary(key) do
        assert DefaultKeyCodec.encode_key(key) == {:error, :invalid_key}
      end
    end
  end

  property "decode_key/1 returns {:ok, encoded_key} for binary encoded_keys" do
    check all(encoded_key <- binary()) do
      assert DefaultKeyCodec.decode_key(encoded_key) == {:ok, encoded_key}
    end
  end

  property "decode_key/1 returns {:error, :invalid_key} for non-binary encoded_keys" do
    check all(encoded_key <- term()) do
      if not is_binary(encoded_key) do
        assert DefaultKeyCodec.decode_key(encoded_key) == {:error, :invalid_key}
      end
    end
  end
end
